package dup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"math"
	"math/bits"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// videoFrameCount is the number of frames sampled per video for fingerprinting.
const videoFrameCount = 8

// Per-subprocess timeouts. These only bound a *hung* ffmpeg/ffprobe (e.g. a
// corrupt file) — normal calls finish far sooner. User cancellation is separate
// and immediate, carried by the scan context passed into each call.
const (
	probeTimeout   = 30 * time.Second
	extractTimeout = 60 * time.Second
)

// singlePassMaxDuration is the cutoff below which fingerprinting uses one
// single-pass ffmpeg decode (fast for short videos). Above it, a full decode
// would cost more than N targeted fast seeks, so the per-frame path is used.
// Benchmarks put the crossover near ~210s; 300s leaves headroom.
const singlePassMaxDuration = 300.0

// videoThreadLimit caps the internal decode threads of EACH ffmpeg process during
// a scan. ffmpeg defaults to using every core, so without this cap running several
// fingerprint workers at once spawns workers×cores threads and freezes the machine.
// runScan sets this (with setVideoThreadLimit) before fingerprinting starts.
var videoThreadLimit int32 = 2

// setVideoThreadLimit records the per-process ffmpeg thread cap for this scan.
func setVideoThreadLimit(n int) {
	if n < 1 {
		n = 1
	}
	atomic.StoreInt32(&videoThreadLimit, int32(n))
}

// vThreads returns the per-process ffmpeg thread cap as a string for -threads.
func vThreads() string {
	n := atomic.LoadInt32(&videoThreadLimit)
	if n < 1 {
		n = 1
	}
	return strconv.FormatInt(int64(n), 10)
}

// packetSigBins is the fixed length of the compressed-domain packet signature.
// 64 bins gives enough temporal resolution to tell different videos apart while
// staying tiny (256 bytes) to store and instant to compare.
const packetSigBins = 64

// packetFingerprint builds a compressed-domain signature WITHOUT decoding any
// pixels. It asks ffprobe for the byte size of every video packet — read straight
// from the container index, not by decoding — and folds that "frame size over
// time" series into a fixed-length, L1-normalized vector. Two copies, remuxes,
// renames or container swaps of the same encode yield an (almost) identical
// vector; a re-encode at different quality diverges. Cost is one ffprobe with no
// decode: milliseconds of CPU and a few KB of RAM, vs a multi-frame pixel decode.
func packetFingerprint(ctx context.Context, path string) ([]float32, bool) {
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, ffprobeBin(),
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "packet=size",
		"-of", "csv=p=0", path)
	setLowPriority(cmd)
	out, err := cmd.Output()
	if err != nil {
		return nil, false
	}
	sizes := make([]float64, 0, 1024)
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if i := strings.IndexByte(line, ','); i >= 0 { // tolerate trailing fields
			line = line[:i]
		}
		if v, e := strconv.ParseFloat(line, 64); e == nil {
			sizes = append(sizes, v)
		}
	}
	if len(sizes) < packetSigBins { // too few packets to be a meaningful signature
		return nil, false
	}
	bins := make([]float64, packetSigBins)
	n := len(sizes)
	for i, s := range sizes {
		bins[i*packetSigBins/n] += s
	}
	var tot float64
	for _, b := range bins {
		tot += b
	}
	if tot <= 0 {
		return nil, false
	}
	sig := make([]float32, packetSigBins)
	for i, b := range bins {
		sig[i] = float32(b / tot)
	}
	return sig, true
}

// packetSignatureSimilarity is the cosine similarity (0..1) of two packet
// signatures. ~1.0 means the same underlying encode (copy / remux / rename /
// container swap / trim); lower means a different encode (re-encode, rescale).
func packetSignatureSimilarity(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// bytesWithin reports whether two byte sizes are within tol (fraction) of the
// larger. Used by the conservative no-decode tier: a literal copy is exact and a
// remux/rename differs only by container overhead (<2%), so a tight tolerance
// keeps unrelated videos out of the no-decode cluster.
func bytesWithin(a, b int64, tol float64) bool {
	if a <= 0 || b <= 0 {
		return false
	}
	hi := a
	if b > hi {
		hi = b
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return float64(diff)/float64(hi) <= tol
}

// videoToolsAvailable reports whether ffmpeg and ffprobe (configured paths or
// PATH) are both usable. Checked live so UI config changes take effect.
func videoToolsAvailable() bool {
	_, okF := resolveTool(ffmpegBin())
	_, okP := resolveTool(ffprobeBin())
	return okF && okP
}

// parseFFprobeJSON extracts duration (seconds) and the first video stream's
// width/height from ffprobe's JSON output.
func parseFFprobeJSON(data []byte) (duration float64, width, height int, err error) {
	var probe struct {
		Streams []struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err = json.Unmarshal(data, &probe); err != nil {
		return 0, 0, 0, err
	}
	if probe.Format.Duration != "" {
		duration, _ = strconv.ParseFloat(probe.Format.Duration, 64)
	}
	if len(probe.Streams) > 0 {
		width = probe.Streams[0].Width
		height = probe.Streams[0].Height
	}
	if duration <= 0 {
		return 0, width, height, fmt.Errorf("no duration in ffprobe output")
	}
	return duration, width, height, nil
}

// probeVideo runs ffprobe to get a video's duration and resolution. The ffprobe
// process is bound to ctx (killed on scan cancel) and a timeout (kills a file
// that makes ffprobe hang).
func probeVideo(ctx context.Context, path string) (duration float64, width, height int, err error) {
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, ffprobeBin(),
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "format=duration:stream=width,height",
		"-of", "json", path)
	setLowPriority(cmd)
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, 0, err
	}
	return parseFFprobeJSON(out)
}

// videoFrameTimestamps returns n timestamps (seconds) evenly spaced between
// 8% and 92% of the duration. Videos shorter than 10s sample 3 frames.
func videoFrameTimestamps(duration float64, n int) []float64 {
	if duration <= 0 || n <= 0 {
		return nil
	}
	if duration < 10 {
		n = 3
	}
	const lo, hi = 0.08, 0.92
	ts := make([]float64, n)
	if n == 1 {
		ts[0] = duration * 0.5
		return ts
	}
	for i := 0; i < n; i++ {
		frac := lo + (hi-lo)*float64(i)/float64(n-1)
		ts[i] = duration * frac
	}
	return ts
}

// avgFrameDistance is the mean per-frame Hamming distance between two
// fingerprints, comparing the first min(len) aligned frames. Returns a large
// sentinel when either fingerprint is empty.
func avgFrameDistance(a, b []uint64) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return 1 << 30
	}
	total := 0
	for i := 0; i < n; i++ {
		total += bits.OnesCount64(a[i] ^ b[i])
	}
	return total / n
}

// durationTolerance is max(1s, 1% of duration).
func durationTolerance(d float64) float64 {
	t := d * 0.01
	if t < 1.0 {
		t = 1.0
	}
	return t
}

// durationsClose reports whether two durations are within tolerance.
func durationsClose(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= durationTolerance((a+b)/2)
}

// pHashFrame wraps 32x32 grayscale raw bytes as an image and returns its pHash.
func pHashFrame(pix []byte) uint64 {
	gray := &image.Gray{Pix: pix, Stride: 32, Rect: image.Rect(0, 0, 32, 32)}
	return pHashFast(gray)
}

// extractFrameHash seeks to ts (fast input-seek) and pulls one frame scaled to
// 32x32 grayscale raw bytes, then computes its pHash. Returns false on failure.
func extractFrameHash(ctx context.Context, path string, ts float64) (uint64, bool) {
	cctx, cancel := context.WithTimeout(ctx, extractTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, ffmpegBin(),
		"-hide_banner", "-loglevel", "error",
		"-threads", vThreads(),
		"-ss", strconv.FormatFloat(ts, 'f', 3, 64),
		"-i", path,
		"-frames:v", "1",
		"-vf", "scale=32:32",
		"-pix_fmt", "gray",
		"-f", "rawvideo", "-")
	setLowPriority(cmd)
	out, err := cmd.Output()
	if err != nil || len(out) < 32*32 {
		return 0, false
	}
	return pHashFrame(out[:32*32]), true
}

// extractFrameHashKeyframe is the keyframe-only counterpart of extractFrameHash for
// long videos: -skip_frame nokey makes the decoder emit the keyframe at the fast-seek
// position WITHOUT decoding the rest of the GOP forward to the exact timestamp, so it
// costs one keyframe decode instead of up to a full group of pictures. The hashed
// frame is the keyframe at-or-before ts (near the fixed fraction); two copies align.
func extractFrameHashKeyframe(ctx context.Context, path string, ts float64) (uint64, bool) {
	cctx, cancel := context.WithTimeout(ctx, extractTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, ffmpegBin(),
		"-hide_banner", "-loglevel", "error",
		"-threads", vThreads(),
		"-ss", strconv.FormatFloat(ts, 'f', 3, 64),
		"-skip_frame", "nokey",
		"-i", path,
		"-frames:v", "1",
		"-vf", "scale=32:32",
		"-pix_fmt", "gray",
		"-f", "rawvideo", "-")
	setLowPriority(cmd)
	out, err := cmd.Output()
	if err != nil || len(out) < 32*32 {
		return 0, false
	}
	return pHashFrame(out[:32*32]), true
}

// posterTimestamp picks a seek point for a representative still: ~20% into the
// video (past common black intros), clamped so very long videos don't seek too
// far and unknown-length videos still yield a frame.
func posterTimestamp(duration float64) float64 {
	if duration <= 0 {
		return 1.0 // unknown length — a 1s seek usually lands past the first black frame
	}
	ts := duration * 0.2
	if ts > 60 {
		ts = 60
	}
	return ts
}

// extractFramePoster pulls one representative frame from a video, scaled to fit
// thumbStdSize, and writes it as a JPEG to dst (the thumbCachePath). It reuses
// the same ffmpeg discipline as frame-hash extraction: fast input-seek, capped
// threads, below-normal priority, and a hung-process timeout. Returns false on
// any failure so the caller can fall back to the file-type icon.
func extractFramePoster(ctx context.Context, path, dst string) bool {
	dur, _, _, err := probeVideo(ctx, path)
	if err != nil {
		dur = 0 // fall back to a fixed seek
	}
	ts := posterTimestamp(dur)

	cctx, cancel := context.WithTimeout(ctx, extractTimeout)
	defer cancel()
	scale := fmt.Sprintf("scale=w=%d:h=%d:force_original_aspect_ratio=decrease", thumbStdSize, thumbStdSize)
	cmd := exec.CommandContext(cctx, ffmpegBin(),
		"-hide_banner", "-loglevel", "error",
		"-threads", vThreads(),
		"-ss", strconv.FormatFloat(ts, 'f', 3, 64),
		"-i", path,
		"-frames:v", "1",
		"-vf", scale,
		"-f", "mjpeg", "-")
	setLowPriority(cmd)
	out, err := cmd.Output()
	if err != nil || len(out) == 0 {
		return false
	}
	if err := writeThumbAtomic(dst, out); err != nil {
		logf("DEBUG", "Poster cache write failed: %s: %v", path, err)
		return false
	}
	return true
}

// extractFramesSinglePass samples n evenly-spaced frames from the [8%, 92%]
// window of a video in ONE ffmpeg process (a single decode pass) rather than
// launching one process per frame. This is the speed win: benchmarks show the
// per-frame method is dominated by N process launches, not decoding, so doing it
// once is ~5x faster on short videos (the common case here).
//
// It fast-seeks to the window start (-ss before -i) and stops after n frames
// (-frames:v), so only the [8%,92%] span is decoded. The fps filter is tuned so
// the n output frames land at the same fractional positions the per-frame path
// targets. Returns ok=false — caller falls back to per-frame — if ffmpeg emits
// fewer than n frames (e.g. an odd/short/corrupt file).
func extractFramesSinglePass(ctx context.Context, path string, duration float64, n int) ([]uint64, bool) {
	if duration <= 0 || n < 1 {
		return nil, false
	}
	cctx, cancel := context.WithTimeout(ctx, extractTimeout)
	defer cancel()

	const lo, hi = 0.08, 0.92
	start := duration * lo
	span := duration * (hi - lo)
	// 1/rate = span/(n-1) places frame k at start + k*span/(n-1), so the first
	// is at 8% and the last at 92% — matching videoFrameTimestamps. We fast-seek
	// to 8% (-ss) and let -frames:v stop the decode after the nth frame (~92%),
	// so only the [8%,92%] span is decoded. (A -t window-limit is intentionally
	// NOT used: it clips the final boundary frame, yielding n-1 frames.)
	rate := 1.0 / span
	if n > 1 {
		rate = float64(n-1) / span
	}

	cmd := exec.CommandContext(cctx, ffmpegBin(),
		"-hide_banner", "-loglevel", "error",
		"-threads", vThreads(),
		"-ss", strconv.FormatFloat(start, 'f', 3, 64),
		"-i", path,
		"-vf", "fps="+strconv.FormatFloat(rate, 'f', 6, 64)+",scale=32:32,format=gray",
		"-frames:v", strconv.Itoa(n),
		"-pix_fmt", "gray",
		"-f", "rawvideo", "-")
	setLowPriority(cmd)
	out, err := cmd.Output()

	const frameBytes = 32 * 32
	if err != nil || len(out) < n*frameBytes {
		return nil, false
	}
	hashes := make([]uint64, n)
	for i := 0; i < n; i++ {
		hashes[i] = pHashFrame(out[i*frameBytes : (i+1)*frameBytes])
	}
	return hashes, true
}

// ptsTimeRe pulls the pts_time value out of ffmpeg's showinfo log lines.
var ptsTimeRe = regexp.MustCompile(`pts_time:\s*([0-9]+\.?[0-9]*)`)

// extractFramesKeyframe fingerprints a video by decoding ONLY its keyframes
// (-skip_frame nokey skips all P/B-frame decoding) and then choosing the keyframe
// nearest each of the n fixed sampling points. This keeps the same fixed-fraction
// alignment as the fps path — so two copies still match frame-for-frame — while
// decoding a tiny fraction of the frames (benchmarks: ~10x faster on a 1080p clip,
// and the gap widens with resolution and length).
//
// showinfo reports every decoded keyframe's pts_time on stderr; the raw 32x32 gray
// frames arrive on stdout in the same order, so index i of each stream is the same
// keyframe. Returns ok=false (caller falls back to the full fps decode) if the two
// streams disagree in length or fewer than 3 keyframes are usable — so accuracy is
// never worse than the previous behaviour, only faster.
func extractFramesKeyframe(ctx context.Context, path string, duration float64, n int) ([]uint64, bool) {
	if duration <= 0 || n < 1 {
		return nil, false
	}
	cctx, cancel := context.WithTimeout(ctx, extractTimeout)
	defer cancel()

	// -loglevel info is required for showinfo's lines to be emitted on stderr;
	// -vsync passthrough keeps the output frame count equal to the decoded-keyframe
	// count so stdout frames and showinfo lines stay index-aligned.
	var stderr bytes.Buffer
	cmd := exec.CommandContext(cctx, ffmpegBin(),
		"-hide_banner", "-loglevel", "info",
		"-threads", vThreads(),
		"-skip_frame", "nokey",
		"-i", path,
		"-an",
		"-vf", "scale=32:32,format=gray,showinfo",
		"-vsync", "passthrough",
		"-f", "rawvideo", "-")
	setLowPriority(cmd)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, false
	}

	const frameBytes = 32 * 32
	nFrames := len(out) / frameBytes
	if nFrames < 3 {
		return nil, false
	}
	// One pts_time per decoded keyframe, in output order.
	matches := ptsTimeRe.FindAllStringSubmatch(stderr.String(), -1)
	if len(matches) != nFrames {
		return nil, false // counts disagree — alignment is unsafe, fall back
	}
	times := make([]float64, nFrames)
	for i, m := range matches {
		t, perr := strconv.ParseFloat(m[1], 64)
		if perr != nil {
			return nil, false
		}
		times[i] = t
	}

	// For each fixed sampling point, hash the keyframe whose timestamp is nearest.
	ts := videoFrameTimestamps(duration, n)
	hashes := make([]uint64, 0, len(ts))
	for _, target := range ts {
		best, bestDist := 0, math.Inf(1)
		for i, kt := range times {
			if d := math.Abs(kt - target); d < bestDist {
				best, bestDist = i, d
			}
		}
		hashes = append(hashes, pHashFrame(out[best*frameBytes:(best+1)*frameBytes]))
	}
	return hashes, true
}

// fingerprintVideo extracts videoFrameCount frames and returns their pHashes.
// Returns nil if fewer than 3 frames could be extracted (unusable).
//
// Medium videos (the common case): decode only keyframes in one pass and pick the one
// nearest each sampling point (extractFramesKeyframe) — far less decode work for the
// same fixed-fraction alignment; falls back to a full single-pass decode if keyframes
// are too sparse. Long videos (> singlePassMaxDuration) have too many keyframes to
// decode them all at once, so each sample point is a targeted keyframe seek
// (extractFrameHashKeyframe) that grabs the keyframe there without decoding a whole GOP
// forward to the exact frame. Exact per-frame decode is the final fallback either way.
func fingerprintVideo(ctx context.Context, path string, duration float64) []uint64 {
	ts := videoFrameTimestamps(duration, videoFrameCount)
	n := len(ts)
	if n == 0 {
		return nil
	}
	if duration > 0 && duration <= singlePassMaxDuration {
		if hashes, ok := extractFramesKeyframe(ctx, path, duration, n); ok && len(hashes) >= 3 {
			return hashes
		}
		if hashes, ok := extractFramesSinglePass(ctx, path, duration, n); ok && len(hashes) >= 3 {
			return hashes
		}
	} else {
		// Long videos: a targeted keyframe seek per sample point — grabs the keyframe
		// at that point without decoding a whole GOP forward to the exact frame.
		hashes := make([]uint64, 0, n)
		for _, t := range ts {
			if h, ok := extractFrameHashKeyframe(ctx, path, t); ok {
				hashes = append(hashes, h)
			}
		}
		if len(hashes) >= 3 {
			return hashes
		}
	}

	// Final fallback (either path): exact per-frame decode.
	hashes := make([]uint64, 0, n)
	for _, t := range ts {
		if h, ok := extractFrameHash(ctx, path, t); ok {
			hashes = append(hashes, h)
		}
	}
	if len(hashes) < 3 {
		return nil
	}
	return hashes
}

// fingerprintCandidates returns a bool per input duration: true if at least one
// other video shares a close duration. Videos alone in their duration bucket
// cannot have a visual twin, so they skip the expensive frame extraction.
func fingerprintCandidates(durations []float64) []bool {
	n := len(durations)
	out := make([]bool, n)
	if n < 2 {
		return out
	}
	// Sort indices by duration, then scan consecutive pairs once. An element has
	// a close partner iff its immediate sorted neighbor is close: the duration
	// gap grows with slope 1 while durationTolerance grows with slope <=0.005, so
	// once the next neighbor is out of tolerance every later one is too. This is
	// O(n log n) vs the previous O(n²) all-pairs scan, with identical results.
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return durations[idx[a]] < durations[idx[b]] })
	for k := 0; k+1 < n; k++ {
		if durationsClose(durations[idx[k]], durations[idx[k+1]]) {
			out[idx[k]] = true
			out[idx[k+1]] = true
		}
	}
	return out
}

// groupVideosByFingerprint clusters videos whose durations are close AND whose
// average per-frame Hamming distance is within maxAvgDist, using union-find.
// Input should contain only videos that have a fingerprint.
func groupVideosByFingerprint(infos []*ImageInfo, maxAvgDist int) [][]*ImageInfo {
	n := len(infos)
	if n < 2 {
		return nil
	}
	parent := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(x int) int {
		for parent[x] != x {
			parent[x] = parent[parent[x]]
			x = parent[x]
		}
		return x
	}
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if !durationsClose(infos[i].Duration, infos[j].Duration) {
				continue
			}
			if avgFrameDistance(infos[i].VHashes, infos[j].VHashes) <= maxAvgDist {
				if ri, rj := find(i), find(j); ri != rj {
					parent[ri] = rj
				}
			}
		}
	}
	byRoot := make(map[int][]*ImageInfo)
	for i := 0; i < n; i++ {
		r := find(i)
		byRoot[r] = append(byRoot[r], infos[i])
	}
	var out [][]*ImageInfo
	for _, g := range byRoot {
		if len(g) >= 2 {
			out = append(out, g)
		}
	}
	return out
}

// videoGroupSimilarity is the mean (1 - avgDist/64) over all pairs in a group.
func videoGroupSimilarity(grp []*ImageInfo) float64 {
	var sum float64
	var cnt int
	for a := 0; a < len(grp); a++ {
		for b := a + 1; b < len(grp); b++ {
			d := avgFrameDistance(grp[a].VHashes, grp[b].VHashes)
			sum += 1.0 - float64(d)/64.0
			cnt++
		}
	}
	if cnt == 0 {
		return 1.0
	}
	return sum / float64(cnt)
}

// produceVideoInfos probes durations/resolution, then fingerprints only the
// videos that share a duration bucket. Honors cache, progress, and cancellation.
// Mirrors the scaffolding of the image pipeline in runScan.
func produceVideoInfos(ctx context.Context, files []*FileEntry, req ScanRequest, workerCount int) []*ImageInfo {
	n := len(files)
	infos := make([]*ImageInfo, n)

	mp := func(w, h int) float64 {
		return math.Round(float64(w)*float64(h)/100000) / 10
	}

	// ── Phase A: probe duration + resolution (parallel) ──
	setStatus("Probing video durations...")
	logf("DEBUG", "Phase A (probe): %d videos across %d workers", n, workerCount)
	phaseStart := time.Now()
	var probeIdx int64 = -1
	var cacheHitA, probedA, probeFailA int64
	var wgA sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wgA.Add(1)
		go func() {
			defer wgA.Done()
			for {
				i := atomic.AddInt64(&probeIdx, 1)
				if i >= int64(n) {
					return
				}
				if atomic.LoadInt64(&cancelScan) != 0 {
					return // cancelled — stop claiming work
				}
				f := files[i]
				ext := strings.ToLower(filepath.Ext(f.Path))
				info := &ImageInfo{
					Path: f.Path, Name: filepath.Base(f.Path), Dir: filepath.Dir(f.Path),
					Size: f.Size, Format: strings.TrimPrefix(ext, "."), ModTime: f.ModTime,
				}
				if req.UseCache {
					if ce, ok := cache.Get(f.Path, f.Size, f.ModTime); ok {
						info.Duration = ce.Duration
						info.Quick = ce.Quick // carry content identity across phases
						if ce.Width > 0 {
							info.Width, info.Height, info.Megapixel = ce.Width, ce.Height, mp(ce.Width, ce.Height)
						}
						if ce.HasVHashes {
							info.VHashes = ce.VHashes
						}
					}
				}
				if info.Duration == 0 {
					t0 := time.Now()
					d, vw, vh, err := probeVideo(ctx, f.Path)
					if dt := time.Since(t0); dt > 2*time.Second {
						logf("DEBUG", "slow ffprobe: %s took %.1fs", info.Name, dt.Seconds())
					}
					if err == nil {
						atomic.AddInt64(&probedA, 1)
						info.Duration = d
						if vw > 0 {
							info.Width, info.Height, info.Megapixel = vw, vh, mp(vw, vh)
						}
						// Content identity (cheap partial hash) so a moved video reuses
						// its fingerprints; computed once and reused by the packet- and
						// frame-fingerprint phases below.
						if info.Quick == "" {
							info.Quick = contentID(f.Path, f.Size)
						}
						if req.UseCache {
							cache.Set(f.Path, &CacheEntry{
								Size: f.Size, ModUnix: f.ModTime.UnixNano(),
								Width: info.Width, Height: info.Height, Format: info.Format,
								Duration: d, Quick: info.Quick,
							})
						}
					} else {
						atomic.AddInt64(&probeFailA, 1)
						logf("WARN", "ffprobe failed for %s: %v", f.Path, err)
					}
				} else {
					atomic.AddInt64(&cacheHitA, 1) // duration came from cache
				}
				infos[i] = info
				atomic.AddInt64(&procCount, 1) // probe progress (Phase A)
			}
		}()
	}
	wgA.Wait()
	logf("DEBUG", "Phase A (probe) done in %.1fs: %d cached, %d probed, %d failed",
		time.Since(phaseStart).Seconds(),
		atomic.LoadInt64(&cacheHitA), atomic.LoadInt64(&probedA), atomic.LoadInt64(&probeFailA))

	// Reset the progress counter so Phase B (fingerprinting) sweeps from 0 again.
	// The status label ("Probing..." → "Fingerprinting...") tells the user which
	// pass is running; without counting Phase A the bar sat frozen at 0 during
	// the entire probe and looked hung.
	atomic.StoreInt64(&procCount, 0)

	// ── Determine which videos need fingerprinting (shared duration bucket) ──
	durs := make([]float64, n)
	for i, inf := range infos {
		if inf != nil {
			durs[i] = inf.Duration
		}
	}
	cand := fingerprintCandidates(durs)
	nCand := 0
	for _, c := range cand {
		if c {
			nCand++
		}
	}
	logf("DEBUG", "Fingerprint candidates: %d of %d videos share a duration bucket", nCand, n)

	// ── Phase A2: compressed-domain packet signatures (NO pixel decode) ──
	// One ffprobe per candidate yields a packet-size-over-time signature read
	// straight from the container index. This is the cheap no-decode tier.
	setStatus("Reading video signatures...")
	psStart := time.Now()
	var psIdx int64 = -1
	var psDone, psCached int64
	var wgPS sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wgPS.Add(1)
		go func() {
			defer wgPS.Done()
			for {
				i := atomic.AddInt64(&psIdx, 1)
				if i >= int64(n) {
					return
				}
				if atomic.LoadInt64(&cancelScan) != 0 {
					return
				}
				inf := infos[i]
				if inf != nil && cand[i] && inf.Duration > 0 {
					if req.UseCache && len(inf.PSig) == 0 {
						if ce, ok := cache.Get(inf.Path, inf.Size, inf.ModTime); ok && ce.HasPSig {
							inf.PSig = ce.PSig
						}
					}
					if len(inf.PSig) == 0 {
						if sig, ok := packetFingerprint(ctx, inf.Path); ok {
							inf.PSig = sig
							atomic.AddInt64(&psDone, 1)
							if req.UseCache {
								ce := &CacheEntry{
									Size: inf.Size, ModUnix: inf.ModTime.UnixNano(),
									Width: inf.Width, Height: inf.Height, Format: inf.Format,
									Duration: inf.Duration, PSig: inf.PSig, HasPSig: true,
									Quick: inf.Quick,
								}
								if len(inf.VHashes) > 0 {
									ce.VHashes, ce.HasVHashes = inf.VHashes, true
								}
								cache.Set(inf.Path, ce)
							}
						}
					} else {
						atomic.AddInt64(&psCached, 1)
					}
				}
				atomic.AddInt64(&procCount, 1)
			}
		}()
	}
	wgPS.Wait()
	logf("DEBUG", "Phase A2 (packet signature) done in %.1fs: %d computed, %d from cache",
		time.Since(psStart).Seconds(), atomic.LoadInt64(&psDone), atomic.LoadInt64(&psCached))

	// ── Conservative no-decode pre-grouping ──
	// Cluster candidates whose packet signatures match almost exactly AND whose
	// byte size and duration also match: a literal copy or a remux/rename of the
	// SAME encode. These are confident duplicates that need NO pixel decoding —
	// only ONE representative per cluster is fingerprinted (Phase B) and its
	// frame-hash is shared by every member (decoded frames are identical).
	const psMatch = 0.9995 // cosine ≥ this ⇒ same encode (copy/remux). Conservative.
	const sizeTol = 0.02   // byte size within 2% (container-overhead headroom)
	parent := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(x int) int {
		for parent[x] != x {
			parent[x] = parent[parent[x]]
			x = parent[x]
		}
		return x
	}
	cl := make([]int, 0, nCand)
	for i := 0; i < n; i++ {
		if cand[i] && infos[i] != nil && len(infos[i].PSig) > 0 {
			cl = append(cl, i)
		}
	}
	for a := 0; a < len(cl); a++ {
		for b := a + 1; b < len(cl); b++ {
			i, j := cl[a], cl[b]
			if !durationsClose(infos[i].Duration, infos[j].Duration) {
				continue
			}
			if !bytesWithin(infos[i].Size, infos[j].Size, sizeTol) {
				continue
			}
			if packetSignatureSimilarity(infos[i].PSig, infos[j].PSig) >= psMatch {
				if ri, rj := find(i), find(j); ri != rj {
					parent[ri] = rj
				}
			}
		}
	}
	// Choose one representative per cluster — prefer a member that already holds a
	// cached frame-hash so even the representative may skip decoding.
	comp := make(map[int][]int)
	for _, i := range cl {
		r := find(i)
		comp[r] = append(comp[r], i)
	}
	isRep := make([]bool, n)
	repOf := make([]int, n)
	for i := range repOf {
		repOf[i] = -1
	}
	var noDecodeMembers int
	for _, members := range comp {
		rep := members[0]
		for _, m := range members {
			if len(infos[m].VHashes) > 0 {
				rep = m
				break
			}
		}
		isRep[rep] = true
		for _, m := range members {
			repOf[m] = rep
			if m != rep {
				noDecodeMembers++
			}
		}
	}
	logf("INFO", "No-decode tier: %d candidate(s) grouped as copies/remuxes — skipping their frame decode", noDecodeMembers)

	// Reset progress so the fingerprint pass sweeps the bar from 0 again.
	atomic.StoreInt64(&procCount, 0)

	// ── Phase B: fingerprint ONE representative per cluster (the decode tier) ──
	setStatus("Fingerprinting videos...")
	logf("DEBUG", "Phase B (fingerprint): %d candidates across %d workers", nCand, workerCount)
	phaseStart = time.Now()
	var fpIdx int64 = -1
	var fpDone, fpCached int64
	var wgB sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wgB.Add(1)
		go func() {
			defer wgB.Done()
			for {
				i := atomic.AddInt64(&fpIdx, 1)
				if i >= int64(n) {
					return
				}
				if atomic.LoadInt64(&cancelScan) != 0 {
					return // cancelled — stop claiming work
				}
				inf := infos[i]
				if inf != nil && inf.Duration > 0 && cand[i] && isRep[i] && len(inf.VHashes) == 0 {
					t0 := time.Now()
					inf.VHashes = fingerprintVideo(ctx, inf.Path, inf.Duration)
					if dt := time.Since(t0); dt > 3*time.Second {
						logf("DEBUG", "slow fingerprint: %s (%.0fs video) took %.1fs", inf.Name, inf.Duration, dt.Seconds())
					}
					if len(inf.VHashes) > 0 {
						atomic.AddInt64(&fpDone, 1)
						if req.UseCache {
							ce := &CacheEntry{
								Size: inf.Size, ModUnix: inf.ModTime.UnixNano(),
								Width: inf.Width, Height: inf.Height, Format: inf.Format,
								Duration: inf.Duration, VHashes: inf.VHashes, HasVHashes: true,
								Quick: inf.Quick,
							}
							if len(inf.PSig) > 0 {
								ce.PSig, ce.HasPSig = inf.PSig, true
							}
							cache.Set(inf.Path, ce)
						}
					}
				} else if inf != nil && cand[i] && len(inf.VHashes) > 0 {
					atomic.AddInt64(&fpCached, 1) // fingerprint came from cache
				}
				atomic.AddInt64(&procCount, 1)
			}
		}()
	}
	wgB.Wait()
	logf("DEBUG", "Phase B (fingerprint) done in %.1fs: %d fingerprinted, %d from cache",
		time.Since(phaseStart).Seconds(), atomic.LoadInt64(&fpDone), atomic.LoadInt64(&fpCached))

	// Share each representative's frame-hash with its no-decode cluster members.
	// They are byte copies / remuxes of the representative, so its decoded frames
	// are theirs too — this groups them without ever decoding them.
	for _, i := range cl {
		if !isRep[i] && len(infos[i].VHashes) == 0 {
			if rep := repOf[i]; rep >= 0 && len(infos[rep].VHashes) > 0 {
				infos[i].VHashes = infos[rep].VHashes
			}
		}
	}

	out := make([]*ImageInfo, 0, n)
	for _, inf := range infos {
		if inf != nil {
			out = append(out, inf)
		}
	}
	return out
}

// groupVideoDuplicates builds DuplicateGroups from video fingerprints.
func groupVideoDuplicates(images []*ImageInfo, maxAvgDist int) []*DuplicateGroup {
	fp := make([]*ImageInfo, 0, len(images))
	for _, im := range images {
		if len(im.VHashes) > 0 {
			fp = append(fp, im)
		}
	}
	raw := groupVideosByFingerprint(fp, maxAvgDist)

	groups := make([]*DuplicateGroup, 0, len(raw))
	gid := 0
	for _, grp := range raw {
		sort.Slice(grp, func(a, b int) bool { return grp[a].ModTime.Before(grp[b].ModTime) })
		var total int64
		for _, im := range grp {
			total += im.Size
		}
		wasted := total - largestSize(grp)
		if wasted < 0 {
			wasted = 0
		}
		groups = append(groups, &DuplicateGroup{
			ID: gid, Images: grp, Exact: false, Algorithm: "video-frame",
			Similarity: videoGroupSimilarity(grp), TotalSize: total, WastedSize: wasted,
		})
		gid++
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].WastedSize > groups[j].WastedSize })
	return groups
}
