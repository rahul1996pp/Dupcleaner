package dup

import (
	"context"
	"encoding/json"
	"fmt"
	"math/bits"
	"net/http"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// fpcalcTimeout bounds a single fpcalc run. It uses -length 0 (fingerprint the
// whole file), so this only catches a truly hung/corrupt file, not slow ones.
const fpcalcTimeout = 120 * time.Second

// fpcalcBin returns the configured fpcalc path, or "fpcalc" for PATH lookup.
func fpcalcBin() string {
	toolsCfgMu.RLock()
	defer toolsCfgMu.RUnlock()
	if toolsCfg.FPcalc != "" {
		return toolsCfg.FPcalc
	}
	return "fpcalc"
}

// audioToolsAvailable reports whether fpcalc (configured path or PATH) is
// usable. Checked live so UI config changes take effect.
func audioToolsAvailable() bool {
	_, ok := resolveTool(fpcalcBin())
	return ok
}

// parseFpcalcJSON extracts duration (seconds) and the raw fingerprint from
// fpcalc's JSON output. Returns an error if duration<=0 or the fingerprint is
// empty.
func parseFpcalcJSON(data []byte) (duration float64, fingerprint []uint32, err error) {
	var out struct {
		Duration    float64  `json:"duration"`
		Fingerprint []uint32 `json:"fingerprint"`
	}
	if err = json.Unmarshal(data, &out); err != nil {
		return 0, nil, err
	}
	if out.Duration <= 0 {
		return 0, nil, fmt.Errorf("no duration in fpcalc output")
	}
	if len(out.Fingerprint) == 0 {
		return 0, nil, fmt.Errorf("empty fingerprint in fpcalc output")
	}
	return out.Duration, out.Fingerprint, nil
}

// fingerprintAudio runs fpcalc to get a track's duration and raw Chromaprint
// fingerprint. Fingerprints shorter than 10 entries are too short to be
// meaningful and are treated as a failure. Returns (0, nil, err) on any failure.
func fingerprintAudio(ctx context.Context, path string) (duration float64, fp []uint32, err error) {
	cctx, cancel := context.WithTimeout(ctx, fpcalcTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, fpcalcBin(), "-json", "-raw", "-length", "0", path)
	setLowPriority(cmd)
	out, err := cmd.Output()
	if err != nil {
		return 0, nil, err
	}
	duration, fp, err = parseFpcalcJSON(out)
	if err != nil {
		return 0, nil, err
	}
	if len(fp) < 10 {
		return 0, nil, fmt.Errorf("fingerprint too short (%d entries)", len(fp))
	}
	return duration, fp, nil
}

// audioFingerprintSimilarity is the fraction of matching bits across the first
// min(len) aligned fingerprint words. Returns 0.0 when either fingerprint is
// empty.
func audioFingerprintSimilarity(a, b []uint32) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return 0.0
	}
	matching := 0
	for i := 0; i < n; i++ {
		matching += 32 - bits.OnesCount32(a[i]^b[i])
	}
	return float64(matching) / float64(n*32)
}

// groupAudiosByFingerprint clusters tracks whose durations are close AND whose
// fingerprint similarity meets simThreshold, using union-find. Input should
// contain only tracks that have a fingerprint.
func groupAudiosByFingerprint(infos []*ImageInfo, simThreshold float64) [][]*ImageInfo {
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
			if audioFingerprintSimilarity(infos[i].AHashes, infos[j].AHashes) >= simThreshold {
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

// audioGroupSimilarity is the mean fingerprint similarity over all pairs in a
// group.
func audioGroupSimilarity(grp []*ImageInfo) float64 {
	var sum float64
	var cnt int
	for a := 0; a < len(grp); a++ {
		for b := a + 1; b < len(grp); b++ {
			sum += audioFingerprintSimilarity(grp[a].AHashes, grp[b].AHashes)
			cnt++
		}
	}
	if cnt == 0 {
		return 1.0
	}
	return sum / float64(cnt)
}

// produceAudioInfos fingerprints audio files (fpcalc does probe + fingerprint in
// one call), then keeps only tracks that share a duration bucket. Honors cache,
// progress, and cancellation. Mirrors produceVideoInfos.
func produceAudioInfos(ctx context.Context, files []*FileEntry, req ScanRequest, workerCount int) []*ImageInfo {
	n := len(files)
	infos := make([]*ImageInfo, n)

	// ── Phase A: probe duration + fingerprint (parallel) ──
	setStatus("Fingerprinting audio...")
	var fpIdx int64 = -1
	var wgA sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wgA.Add(1)
		go func() {
			defer wgA.Done()
			for {
				i := atomic.AddInt64(&fpIdx, 1)
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
					if ce, ok := cache.Get(f.Path, f.Size, f.ModTime); ok && ce.HasAHashes {
						info.Duration = ce.Duration
						info.AHashes = ce.AHashes
					}
				}
				if len(info.AHashes) == 0 {
					if d, fp, err := fingerprintAudio(ctx, f.Path); err == nil {
						info.Duration = d
						info.AHashes = fp
						if req.UseCache {
							// Content identity (cheap partial hash) so a moved track
							// reuses its fingerprint regardless of path.
							cache.Set(f.Path, &CacheEntry{
								Size: f.Size, ModUnix: f.ModTime.UnixNano(),
								Format: info.Format, Duration: info.Duration,
								AHashes: info.AHashes, HasAHashes: true,
								Quick: contentID(f.Path, f.Size),
							})
						}
					} else {
						logf("WARN", "fpcalc failed for %s: %v", f.Path, err)
					}
				}
				infos[i] = info
				atomic.AddInt64(&procCount, 1)
			}
		}()
	}
	wgA.Wait()

	// ── Phase B: keep only tracks sharing a duration bucket ──
	durs := make([]float64, n)
	for i, inf := range infos {
		if inf != nil && inf.Duration > 0 {
			durs[i] = inf.Duration
		}
	}
	cand := fingerprintCandidates(durs)
	for i, inf := range infos {
		if inf == nil {
			continue
		}
		if !cand[i] || inf.Duration <= 0 {
			inf.AHashes = nil // exclude from grouping
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

// groupAudioDuplicates builds DuplicateGroups from audio fingerprints.
func groupAudioDuplicates(images []*ImageInfo, simThreshold float64) []*DuplicateGroup {
	fp := make([]*ImageInfo, 0, len(images))
	for _, im := range images {
		if len(im.AHashes) > 0 {
			fp = append(fp, im)
		}
	}
	raw := groupAudiosByFingerprint(fp, simThreshold)

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
			ID: gid, Images: grp, Exact: false, Algorithm: "audio-chroma",
			Similarity: audioGroupSimilarity(grp), TotalSize: total, WastedSize: wasted,
		})
		gid++
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].WastedSize > groups[j].WastedSize })
	return groups
}

type audioToolsStatusResp struct {
	FPcalcPath     string `json:"fpcalc_path"` // configured (empty = auto)
	FPcalcResolved string `json:"fpcalc_resolved"`
	FPcalcFound    bool   `json:"fpcalc_found"`
}

func currentAudioToolsStatus() audioToolsStatusResp {
	toolsCfgMu.RLock()
	fp := toolsCfg.FPcalc
	toolsCfgMu.RUnlock()
	res, ok := resolveTool(fpcalcBin())
	return audioToolsStatusResp{
		FPcalcPath: fp, FPcalcResolved: res, FPcalcFound: ok,
	}
}

// handleAudioTools: GET returns status; POST sets the fpcalc path (empty = auto).
func handleAudioTools(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost {
		var req struct {
			FPcalc *string `json:"fpcalc"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.FPcalc != nil {
			p := strings.TrimSpace(*req.FPcalc)
			if p != "" && !validateTool(p) {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Not a working fpcalc binary"})
				return
			}
			toolsCfgMu.Lock()
			toolsCfg.FPcalc = p
			toolsCfgMu.Unlock()
		}
		if err := saveToolsConfig(); err != nil {
			logf("WARN", "tools.json save failed: %v", err)
		}
		st := currentAudioToolsStatus()
		logf("INFO", "Audio tool path updated: fpcalc=%q(found=%v)", st.FPcalcResolved, st.FPcalcFound)
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "status": st})
		return
	}
	json.NewEncoder(w).Encode(currentAudioToolsStatus())
}
