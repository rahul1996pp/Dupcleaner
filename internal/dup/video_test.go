package dup

import (
	"bytes"
	"context"
	"image/jpeg"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestParseFFprobeJSON(t *testing.T) {
	data := []byte(`{
		"streams": [{"width": 1920, "height": 1080}],
		"format": {"duration": "123.456"}
	}`)
	dur, w, h, err := parseFFprobeJSON(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dur != 123.456 {
		t.Errorf("duration = %v, want 123.456", dur)
	}
	if w != 1920 || h != 1080 {
		t.Errorf("size = %dx%d, want 1920x1080", w, h)
	}
}

func TestParseFFprobeJSON_NoDuration(t *testing.T) {
	data := []byte(`{"streams": [{"width": 640, "height": 480}], "format": {}}`)
	if _, _, _, err := parseFFprobeJSON(data); err == nil {
		t.Error("expected error when duration missing")
	}
}

func TestVideoFrameTimestamps(t *testing.T) {
	ts := videoFrameTimestamps(100, 8)
	if len(ts) != 8 {
		t.Fatalf("len = %d, want 8", len(ts))
	}
	if ts[0] < 7.9 || ts[0] > 8.1 {
		t.Errorf("first ts = %v, want ~8", ts[0])
	}
	if ts[7] < 91.9 || ts[7] > 92.1 {
		t.Errorf("last ts = %v, want ~92", ts[7])
	}
	for i := 1; i < len(ts); i++ {
		if ts[i] <= ts[i-1] {
			t.Errorf("timestamps not strictly increasing at %d", i)
		}
	}
}

func TestVideoFrameTimestamps_Short(t *testing.T) {
	ts := videoFrameTimestamps(6, 8)
	if len(ts) != 3 {
		t.Errorf("short video len = %d, want 3", len(ts))
	}
}

func TestVideoFrameTimestamps_Invalid(t *testing.T) {
	if videoFrameTimestamps(0, 8) != nil {
		t.Error("zero duration should return nil")
	}
}

func TestAvgFrameDistance(t *testing.T) {
	a := []uint64{0b0000, 0b1111}
	b := []uint64{0b0001, 0b1110}
	// frame0: 1 bit diff, frame1: 1 bit diff → avg 1
	if d := avgFrameDistance(a, b); d != 1 {
		t.Errorf("distance = %d, want 1", d)
	}
}

func TestAvgFrameDistance_DifferentLengths(t *testing.T) {
	a := []uint64{0, 0, 0}
	b := []uint64{0, 0}
	if d := avgFrameDistance(a, b); d != 0 {
		t.Errorf("distance = %d, want 0 (compare min length)", d)
	}
}

func TestAvgFrameDistance_Empty(t *testing.T) {
	if d := avgFrameDistance(nil, []uint64{1}); d < 1000 {
		t.Errorf("empty should return large sentinel, got %d", d)
	}
}

func TestDurationsClose(t *testing.T) {
	if !durationsClose(100.0, 100.5) {
		t.Error("100.0 and 100.5 should be close (tol >= 1s)")
	}
	if durationsClose(100.0, 105.0) {
		t.Error("100.0 and 105.0 should NOT be close")
	}
	if !durationsClose(1000.0, 1005.0) {
		t.Error("1000 and 1005 should be close (1% tol = 10s)")
	}
}

func TestFingerprintCandidates(t *testing.T) {
	durs := []float64{100, 100.4, 250, 500, 500.2}
	got := fingerprintCandidates(durs)
	want := []bool{true, true, false, true, true}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: got %v want %v", i, got[i], want[i])
		}
	}
}

// bruteFingerprintCandidates is the original O(n²) reference implementation,
// kept only to prove the optimized fingerprintCandidates returns identical results.
func bruteFingerprintCandidates(durations []float64) []bool {
	n := len(durations)
	out := make([]bool, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j && durationsClose(durations[i], durations[j]) {
				out[i] = true
				break
			}
		}
	}
	return out
}

func TestFingerprintCandidatesMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 200; trial++ {
		n := rng.Intn(60)
		durs := make([]float64, n)
		for i := range durs {
			// Mix tiny (floor-tolerance regime) and large (linear-tolerance regime)
			// durations, with frequent near-collisions to exercise the window edge.
			base := rng.Float64() * 2000
			if rng.Intn(3) == 0 && i > 0 {
				base = durs[rng.Intn(i)] + (rng.Float64()-0.5)*4 // cluster near an existing one
			}
			durs[i] = base
		}
		got := fingerprintCandidates(durs)
		want := bruteFingerprintCandidates(durs)
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("trial %d index %d (dur=%.4f): got %v want %v\ndurs=%v",
					trial, i, durs[i], got[i], want[i], durs)
			}
		}
	}
}

func makeTestClip(t *testing.T, dir, name, size string) string {
	t.Helper()
	out := filepath.Join(dir, name)
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=duration=5:size="+size+":rate=10",
		"-pix_fmt", "yuv420p", "-y", out)
	if err := cmd.Run(); err != nil {
		t.Fatalf("ffmpeg make clip failed: %v", err)
	}
	return out
}

func TestVideoFingerprintMatchesAcrossResolution(t *testing.T) {
	if !videoToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not available")
	}
	dir := t.TempDir()
	hi := makeTestClip(t, dir, "hi.mp4", "1280x720")
	lo := makeTestClip(t, dir, "lo.mp4", "640x480")

	ctx := context.Background()
	dHi, _, _, err := probeVideo(ctx, hi)
	if err != nil {
		t.Fatalf("probe hi: %v", err)
	}
	dLo, _, _, err := probeVideo(ctx, lo)
	if err != nil {
		t.Fatalf("probe lo: %v", err)
	}
	if !durationsClose(dHi, dLo) {
		t.Fatalf("durations not close: %v vs %v", dHi, dLo)
	}
	fpHi := fingerprintVideo(ctx, hi, dHi)
	fpLo := fingerprintVideo(ctx, lo, dLo)
	if len(fpHi) == 0 || len(fpLo) == 0 {
		t.Fatal("empty fingerprint")
	}
	if d := avgFrameDistance(fpHi, fpLo); d > 10 {
		t.Errorf("same content at different res should match: avg dist %d > 10", d)
	}
}

// TestExtractFramesSinglePass verifies the single-pass extractor returns exactly
// the requested number of frames (the property that keeps frame-alignment matching
// valid) and that its fingerprint matches the same content at a different size.
func TestExtractFramesSinglePass(t *testing.T) {
	if !videoToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not available")
	}
	dir := t.TempDir()
	hi := makeTestClip(t, dir, "hi.mp4", "640x480")
	lo := makeTestClip(t, dir, "lo.mp4", "320x240")
	ctx := context.Background()
	dHi, _, _, err := probeVideo(ctx, hi)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}

	const n = 8
	got, ok := extractFramesSinglePass(ctx, hi, dHi, n)
	if !ok {
		t.Fatal("single-pass extraction failed")
	}
	if len(got) != n {
		t.Fatalf("got %d frames, want exactly %d (frame count must be stable for alignment)", len(got), n)
	}

	// Same content, different resolution → fingerprints should be close.
	dLo, _, _, _ := probeVideo(ctx, lo)
	fHi := fingerprintVideo(ctx, hi, dHi)
	fLo := fingerprintVideo(ctx, lo, dLo)
	if d := avgFrameDistance(fHi, fLo); d > 12 {
		t.Errorf("same content at different res: avg dist %d > 12", d)
	}
}

// BenchmarkExtractFrames measures single-pass vs per-frame extraction so the
// "faster cold scan" claim is backed by evidence, not assumption. Run with:
//
//	go test -run=^$ -bench=BenchmarkExtractFrames -benchtime=10x
func BenchmarkExtractFrames(b *testing.B) {
	if !videoToolsAvailable() {
		b.Skip("ffmpeg/ffprobe not available")
	}
	dir := b.TempDir()
	// A 30s clip so we sample the full videoFrameCount and pay realistic per-call
	// process overhead (the cost the single-pass path eliminates).
	clip := filepath.Join(dir, "bench.mp4")
	mk := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=duration=30:size=640x480:rate=25",
		"-pix_fmt", "yuv420p", "-y", clip)
	if err := mk.Run(); err != nil {
		b.Fatalf("ffmpeg make clip: %v", err)
	}
	ctx := context.Background()
	dur, _, _, err := probeVideo(ctx, clip)
	if err != nil {
		b.Fatalf("probe: %v", err)
	}
	ts := videoFrameTimestamps(dur, videoFrameCount)

	b.Run("singlepass_1process", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, ok := extractFramesSinglePass(ctx, clip, dur, len(ts)); !ok {
				b.Fatal("single-pass failed")
			}
		}
	})
	b.Run("perframe_Nprocesses", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, t := range ts {
				extractFrameHash(ctx, clip, t)
			}
		}
	})
}

// makeKeyframeClip encodes a 20s clip with an explicit GOP so it has many
// keyframes (the short testsrc clips elsewhere have only one, which forces the
// keyframe path to fall back). gop is the -g value as a string.
func makeKeyframeClip(t *testing.T, dir, name, size, gop string) string {
	t.Helper()
	out := filepath.Join(dir, name)
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=duration=20:size="+size+":rate=10",
		"-c:v", "libx264", "-g", gop, "-pix_fmt", "yuv420p", "-y", out)
	if err := cmd.Run(); err != nil {
		t.Fatalf("ffmpeg make keyframe clip failed: %v", err)
	}
	return out
}

// TestExtractFramesKeyframe verifies the keyframe extractor returns exactly the
// requested number of frames and that its fingerprint still matches the same
// content re-encoded at a DIFFERENT resolution AND a DIFFERENT GOP (so keyframes
// land at different timestamps) — the accuracy property that lets keyframe-only
// decoding replace the full-window decode without missing duplicates.
func TestExtractFramesKeyframe(t *testing.T) {
	if !videoToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not available")
	}
	dir := t.TempDir()
	hi := makeKeyframeClip(t, dir, "hi.mp4", "640x480", "10")
	ctx := context.Background()
	dHi, _, _, err := probeVideo(ctx, hi)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}

	const n = 8
	got, ok := extractFramesKeyframe(ctx, hi, dHi, n)
	if !ok {
		t.Fatal("keyframe extraction failed (expected many keyframes with -g 10)")
	}
	if len(got) != n {
		t.Fatalf("got %d frames, want exactly %d (frame count must be stable for alignment)", len(got), n)
	}

	// Same content, different resolution AND different keyframe spacing → match.
	lo := makeKeyframeClip(t, dir, "lo.mp4", "320x240", "7")
	dLo, _, _, _ := probeVideo(ctx, lo)
	fHi := fingerprintVideo(ctx, hi, dHi)
	fLo := fingerprintVideo(ctx, lo, dLo)
	if d := avgFrameDistance(fHi, fLo); d > 12 {
		t.Errorf("keyframe fingerprints across encode should match: avg dist %d > 12", d)
	}
}

// TestFingerprintVideoLongPath exercises the > singlePassMaxDuration branch, which
// fingerprints via targeted keyframe seeks. It uses tiny but genuinely long (>300s)
// clips so fingerprintVideo actually takes that path, and checks the fingerprint is
// usable and still matches the same content re-encoded at a different resolution/GOP.
func TestFingerprintVideoLongPath(t *testing.T) {
	if !videoToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not available")
	}
	dir := t.TempDir()
	mk := func(name, size, gop string) string {
		out := filepath.Join(dir, name)
		cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
			"-f", "lavfi", "-i", "testsrc=duration=305:size="+size+":rate=5",
			"-c:v", "libx264", "-preset", "ultrafast", "-g", gop, "-pix_fmt", "yuv420p", "-y", out)
		if err := cmd.Run(); err != nil {
			t.Fatalf("make long clip %s: %v", name, err)
		}
		return out
	}
	ctx := context.Background()
	hi := mk("hi.mp4", "320x240", "50")
	dHi, _, _, err := probeVideo(ctx, hi)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if dHi <= singlePassMaxDuration {
		t.Fatalf("clip duration %.0fs not > %.0fs; long path wouldn't run", dHi, singlePassMaxDuration)
	}
	fHi := fingerprintVideo(ctx, hi, dHi)
	if len(fHi) < 3 {
		t.Fatalf("long-path fingerprint too short: %d", len(fHi))
	}

	lo := mk("lo.mp4", "160x120", "37")
	dLo, _, _, _ := probeVideo(ctx, lo)
	fLo := fingerprintVideo(ctx, lo, dLo)
	if d := avgFrameDistance(fHi, fLo); d > 12 {
		t.Errorf("long-path fingerprints across encode should match: avg dist %d > 12", d)
	}
}

func TestGroupVideosByFingerprint(t *testing.T) {
	a := &ImageInfo{Path: "a", Duration: 100, VHashes: []uint64{1, 2, 3}}
	b := &ImageInfo{Path: "b", Duration: 100.3, VHashes: []uint64{1, 2, 3}}      // dup of a
	c := &ImageInfo{Path: "c", Duration: 300, VHashes: []uint64{99, 88, 77}}     // alone
	d := &ImageInfo{Path: "d", Duration: 100.2, VHashes: []uint64{0xFF, 0xFF, 0xFF}} // same dur, diff content
	groups := groupVideosByFingerprint([]*ImageInfo{a, b, c, d}, 4)
	if len(groups) != 1 {
		t.Fatalf("got %d groups, want 1", len(groups))
	}
	if len(groups[0]) != 2 {
		t.Fatalf("group size %d, want 2", len(groups[0]))
	}
}

func TestGroupVideosByFingerprint_DurationGate(t *testing.T) {
	// Identical fingerprints but far-apart durations must NOT group.
	a := &ImageInfo{Path: "a", Duration: 100, VHashes: []uint64{1, 2, 3}}
	b := &ImageInfo{Path: "b", Duration: 400, VHashes: []uint64{1, 2, 3}}
	groups := groupVideosByFingerprint([]*ImageInfo{a, b}, 4)
	if len(groups) != 0 {
		t.Fatalf("got %d groups, want 0 (duration gate)", len(groups))
	}
}

func TestPosterTimestamp(t *testing.T) {
	if got := posterTimestamp(0); got != 1.0 {
		t.Errorf("unknown duration ts = %v, want 1.0", got)
	}
	if got := posterTimestamp(100); got < 19.9 || got > 20.1 {
		t.Errorf("ts(100) = %v, want ~20", got)
	}
	if got := posterTimestamp(1000); got != 60 {
		t.Errorf("ts(1000) = %v, want clamped 60", got)
	}
}

func TestExtractFramePoster(t *testing.T) {
	if !videoToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not available")
	}
	dir := t.TempDir()
	clip := makeTestClip(t, dir, "clip.mp4", "640x480")
	dst := filepath.Join(dir, "poster.jpg")

	if !extractFramePoster(context.Background(), clip, dst) {
		t.Fatal("extractFramePoster returned false")
	}
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read poster: %v", err)
	}
	img, err := jpeg.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode poster: %v", err)
	}
	b := img.Bounds()
	if b.Dx() == 0 || b.Dy() == 0 {
		t.Error("poster has zero dimension")
	}
	if b.Dx() > thumbStdSize || b.Dy() > thumbStdSize {
		t.Errorf("poster %dx%d exceeds %d", b.Dx(), b.Dy(), thumbStdSize)
	}
}

func TestExtractFramePoster_BadPath(t *testing.T) {
	if !videoToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not available")
	}
	dir := t.TempDir()
	dst := filepath.Join(dir, "poster.jpg")
	if extractFramePoster(context.Background(), filepath.Join(dir, "nope.mp4"), dst) {
		t.Error("expected false for nonexistent video")
	}
}

// Suppress unused import — time.Time is used in ImageInfo.ModTime above
var _ = time.Time{}

// TestVideoRescanIsPureCacheHit pins the cross-scan caching guarantee for video:
// when the cache already holds a video's probe metadata AND both fingerprints
// (frame VHashes + packet PSig), a re-scan must reuse them and do NO ffprobe/
// ffmpeg work.
//
// The proof is the choice of inputs: the "videos" are plain text files, so they
// can never be probed or fingerprinted. The miss pass therefore yields empty
// fingerprints; the hit pass — after seeding the cache — yields the exact seeded
// values. The only way the fingerprints can appear is straight from cache, which
// is the behaviour we want to lock in. The test is independent of whether ffmpeg
// is installed, so it always runs (no t.Skip).
func TestVideoRescanIsPureCacheHit(t *testing.T) {
	old := cache
	cache = NewCache()
	t.Cleanup(func() { cache = old })

	dir := t.TempDir()
	mk := func(name, content string) *FileEntry {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return &FileEntry{Path: p, Size: st.Size(), ModTime: st.ModTime()}
	}
	// Two candidates that share a duration bucket (so both pass the fingerprint
	// gate) but have distinct fingerprints and sizes (so they are not collapsed
	// as byte-copies and each must pull its own cached fingerprints).
	fa := mk("a.mp4", "not a real video AAAAAA")
	fb := mk("b.mp4", "not a real video B")
	req := ScanRequest{Category: "videos", Mode: "visual", UseCache: true}
	ctx := context.Background()

	// ── Pass 1: empty cache ⇒ miss. Garbage files can't be probed, so there is
	// nothing to fingerprint: durations stay 0 and VHashes/PSig stay empty. ──
	miss := produceVideoInfos(ctx, []*FileEntry{fa, fb}, req, 2)
	for _, inf := range miss {
		if inf.Duration != 0 || len(inf.VHashes) != 0 || len(inf.PSig) != 0 {
			t.Fatalf("cold pass on non-video should yield no fingerprints, got "+
				"dur=%v vhashes=%d psig=%d for %s", inf.Duration, len(inf.VHashes), len(inf.PSig), inf.Name)
		}
	}

	// ── Seed the cache as a prior scan would have, then re-scan. ──
	vhA, vhB := []uint64{1, 2, 3, 4, 5, 6, 7, 8}, []uint64{9, 10, 11, 12, 13, 14, 15, 16}
	psA, psB := []float32{0.1, 0.2, 0.3}, []float32{0.4, 0.5, 0.6}
	seed := func(f *FileEntry, vh []uint64, ps []float32) {
		cache.Set(f.Path, &CacheEntry{
			Size: f.Size, ModUnix: f.ModTime.UnixNano(),
			Width: 1920, Height: 1080, Format: "mp4", Duration: 100.0,
			VHashes: vh, HasVHashes: true,
			PSig: ps, HasPSig: true,
		})
	}
	seed(fa, vhA, psA)
	seed(fb, vhB, psB)

	hit := produceVideoInfos(ctx, []*FileEntry{fa, fb}, req, 2)
	if len(hit) != 2 {
		t.Fatalf("got %d infos, want 2", len(hit))
	}
	byPath := map[string]*ImageInfo{}
	for _, inf := range hit {
		byPath[inf.Path] = inf
	}
	check := func(f *FileEntry, vh []uint64, ps []float32) {
		inf := byPath[f.Path]
		if inf == nil {
			t.Fatalf("missing info for %s", f.Path)
		}
		if inf.Duration != 100.0 {
			t.Errorf("%s duration = %v, want 100.0 from cache", inf.Name, inf.Duration)
		}
		if inf.Width != 1920 || inf.Height != 1080 {
			t.Errorf("%s size = %dx%d, want 1920x1080 from cache", inf.Name, inf.Width, inf.Height)
		}
		if !reflect.DeepEqual(inf.VHashes, vh) {
			t.Errorf("%s VHashes = %v, want cached %v (frame fingerprint was recomputed!)", inf.Name, inf.VHashes, vh)
		}
		if !reflect.DeepEqual(inf.PSig, ps) {
			t.Errorf("%s PSig = %v, want cached %v (packet signature was recomputed!)", inf.Name, inf.PSig, ps)
		}
	}
	check(fa, vhA, psA)
	check(fb, vhB, psB)
}

func TestResolveTool(t *testing.T) {
	if _, ok := resolveTool(""); ok {
		t.Error("empty bin should not resolve")
	}
	tmp := filepath.Join(t.TempDir(), "fakebin")
	if err := os.WriteFile(tmp, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, ok := resolveTool(tmp); !ok {
		t.Error("existing file path should resolve")
	}
	if _, ok := resolveTool(filepath.Join(t.TempDir(), "nope")); ok {
		t.Error("missing path should not resolve")
	}
}

