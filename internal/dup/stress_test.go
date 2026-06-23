package dup

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestStressBenchmark sweeps worker counts across ALL scannable file types
// (images, videos, and audio when fpcalc is present), measuring throughput and
// peak RAM at each setting, then prints a per-category report recommending the
// best Threads/RAM for THIS machine. Gated behind DUPCLEANER_BENCH so a normal
// `go test` run skips it — use bench.bat to run it.
//
//	DUPCLEANER_BENCH=1            required to run
//	DUPCLEANER_BENCH_DIR=<path>   stress your REAL files (else synthetic ones)
//	DUPCLEANER_BENCH_LIMIT=<n>    files per category per sweep (default 80)
//	DUPCLEANER_BENCH_CATS=a,b     restrict categories (e.g. "videos")
func TestStressBenchmark(t *testing.T) {
	if os.Getenv("DUPCLEANER_BENCH") == "" {
		t.Skip("set DUPCLEANER_BENCH=1 to run the stress benchmark (use bench.bat)")
	}

	// Silence per-scan DEBUG/INFO log noise so the report stays readable.
	prevRank := atomic.LoadInt32(&minLogRank)
	atomic.StoreInt32(&minLogRank, 2) // WARN
	defer atomic.StoreInt32(&minLogRank, prevRank)

	limit := benchEnvInt("DUPCLEANER_BENCH_LIMIT", 80)
	if limit < 4 {
		limit = 4
	}
	realDir := os.Getenv("DUPCLEANER_BENCH_DIR")

	cpus := runtime.NumCPU()
	ramGB := float64(physicalRAMBytes()) / (1 << 30)
	workers := benchUniqInts([]int{1, 2, 4, cpus / 2, cpus, cpus * 2})

	// Categories to test, each with the mode that exercises its real workload.
	type catSpec struct{ name, mode string }
	all := []catSpec{
		{"images", "standard"}, // decode + perceptual hash (CPU-bound)
		{"videos", "visual"},   // ffprobe + ffmpeg frame fingerprint
	}
	audioOK := audioToolsAvailable()
	if audioOK {
		all = append(all, catSpec{"audio", "acoustic"}) // fpcalc fingerprint
	}
	if want := os.Getenv("DUPCLEANER_BENCH_CATS"); want != "" {
		keep := make(map[string]bool)
		for _, c := range strings.Split(want, ",") {
			keep[strings.TrimSpace(strings.ToLower(c))] = true
		}
		var filtered []catSpec
		for _, c := range all {
			if keep[c.name] {
				filtered = append(filtered, c)
			}
		}
		all = filtered
	}

	var report strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&report, format, a...) }
	w("\n==================== DupCleaner Benchmark ====================\n")
	w(" Machine : %d logical CPUs, %.1f GB RAM\n", cpus, ramGB)
	if realDir != "" {
		w(" Source  : real files from %s (capped at %d/category)\n", realDir, limit)
	} else {
		w(" Source  : synthetic files (%d per category)\n", limit)
	}
	w(" Sweep   : workers = %v, cache disabled\n", workers)
	if !audioOK {
		w(" Note    : audio not tested (fpcalc not found on PATH)\n")
	}

	type recSummary struct {
		cat     string
		threads int
		peakMB  float64
	}
	var recs []recSummary

	for _, c := range all {
		dir, nFiles := benchPrepare(t, c.name, realDir, limit)
		if nFiles == 0 {
			w("\n[%s] no files available — skipped\n", strings.ToUpper(c.name))
			continue
		}

		type result struct {
			workers    int
			wall       time.Duration
			throughput float64
			peakMB     float64
		}
		var results []result
		for _, wk := range workers {
			debug.FreeOSMemory() // return prior sweep's pages to the OS for a clean RSS baseline
			stop := make(chan struct{})
			peakCh := benchPeakRAM(stop)
			start := time.Now()
			runScan(ScanRequest{
				Dirs: []string{dir}, Category: c.name, Mode: c.mode,
				Threshold: 10, Threads: wk, UseCache: false, SkipHidden: true,
			})
			wall := time.Since(start)
			close(stop)
			peak := <-peakCh
			results = append(results, result{wk, wall, float64(nFiles) / wall.Seconds(), peak})
		}

		bestTp := 0.0
		for _, r := range results {
			if r.throughput > bestTp {
				bestTp = r.throughput
			}
		}
		// Recommend the most RAM-efficient setting within 5% of the best speed.
		rec := results[len(results)-1]
		for _, r := range results {
			if r.throughput >= 0.95*bestTp && r.peakMB < rec.peakMB {
				rec = r
			}
		}

		w("\n[%s] %d files, mode=%s\n", strings.ToUpper(c.name), nFiles, c.mode)
		w(" %-8s %-10s %-13s %-11s %-6s\n", "Workers", "Wall", "Throughput", "Peak RAM", "Speed")
		w(" %-8s %-10s %-13s %-11s %-6s\n", "-------", "----", "----------", "---------", "-----")
		for _, r := range results {
			mark := ""
			if r.workers == rec.workers {
				mark = "  <= recommended"
			}
			w(" %-8d %-10s %-13s %-11s %-6s%s\n",
				r.workers,
				fmt.Sprintf("%.1fs", r.wall.Seconds()),
				fmt.Sprintf("%.1f/s", r.throughput),
				fmt.Sprintf("%.0f MB", r.peakMB),
				fmt.Sprintf("%.2fx", r.throughput/bestTp), mark)
		}
		recs = append(recs, recSummary{c.name, rec.workers, rec.peakMB})
		w("  -> best: Threads=%d, ~%.0f MB peak RAM. %s\n",
			rec.workers, rec.peakMB, benchVerdict(rec.workers, cpus))
	}

	w("\n================== RECOMMENDED SETTINGS ==================\n")
	w(" For THIS machine (%d logical CPUs, %.1f GB RAM):\n", cpus, ramGB)
	for _, r := range recs {
		w("   %-7s Threads=%-3d  RAM ~%.0f MB peak\n", strings.ToUpper(r.cat), r.threads, r.peakMB)
	}
	w(" Set Threads per scan type in the UI (Auto = %d cores).\n", cpus)
	w("==========================================================\n")
	fmt.Print(report.String())
}

func benchVerdict(rec, cpus int) string {
	switch {
	case rec < cpus:
		return "I/O-bound: fewer workers = same speed, less RAM (and less HDD thrash)."
	case rec > cpus:
		return "Oversubscribing past core count helped: workload waits on I/O or external tools."
	default:
		return "CPU-bound: scales with cores — Auto is ideal."
	}
}

// benchPrepare returns a directory holding up to `limit` files of the category
// (real ones copied from realDir, or freshly generated synthetic ones) and the
// file count.
func benchPrepare(t *testing.T, cat, realDir string, limit int) (string, int) {
	t.Helper()
	dir := t.TempDir()
	if realDir != "" {
		src := collectFiles([]string{realDir}, true, 0, extsForCategory(cat), nil)
		if len(src) > limit {
			src = src[:limit]
		}
		n := 0
		for i, f := range src {
			data, err := os.ReadFile(f.Path)
			if err != nil {
				continue
			}
			if os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%04d%s", i, filepath.Ext(f.Path))), data, 0o644) == nil {
				n++
			}
		}
		return dir, n
	}
	switch cat {
	case "images":
		genImages(t, dir, limit)
	case "videos":
		genMediaCopies(t, dir, limit, "mp4",
			[]string{"testsrc", "testsrc2", "smptebars", "rgbtestsrc"},
			func(spec, out string) []string {
				return []string{"-f", "lavfi", "-i", spec + "=duration=12:size=640x480:rate=24",
					"-c:v", "libx264", "-g", "48", "-pix_fmt", "yuv420p", "-y", out}
			})
	case "audio":
		genMediaCopies(t, dir, limit, "mp3",
			[]string{"sine=frequency=440", "sine=frequency=880", "sine=frequency=220", "anoisesrc=color=pink"},
			func(spec, out string) []string {
				return []string{"-f", "lavfi", "-i", spec + ":duration=20", "-y", out}
			})
	}
	return dir, limit
}

// benchEnvInt reads an integer env var, returning def when unset/invalid.
func benchEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// benchUniqInts returns the unique values >=1 from in, sorted ascending.
func benchUniqInts(in []int) []int {
	seen := make(map[int]bool)
	var out []int
	for _, v := range in {
		if v >= 1 && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Ints(out)
	return out
}

// benchPeakRAM polls the whole process tree's working set — DupCleaner plus the
// ffmpeg/ffprobe/fpcalc children the scan spawns — until stop is closed, then
// reports the peak in MB. Off Windows (or if the OS query fails) it falls back
// to Go heap in-use, which counts only in-process memory.
func benchPeakRAM(stop <-chan struct{}) <-chan float64 {
	out := make(chan float64, 1)
	go func() {
		var ms runtime.MemStats
		var peak uint64
		sample := func() {
			rss := procTreeWorkingSetBytes()
			if rss == 0 {
				runtime.ReadMemStats(&ms)
				rss = ms.HeapInuse
			}
			if rss > peak {
				peak = rss
			}
		}
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				sample()
				out <- float64(peak) / 1024 / 1024
				return
			case <-tick.C:
				sample()
			}
		}
	}()
	return out
}

// genImages writes `count` PNGs (4 distinct patterns, rest copies) into dir.
func genImages(t *testing.T, dir string, count int) {
	t.Helper()
	bases := make([][]byte, 4)
	for i := range bases {
		img := image.NewRGBA(image.Rect(0, 0, 256, 256))
		for y := 0; y < 256; y++ {
			for x := 0; x < 256; x++ {
				img.Set(x, y, color.RGBA{uint8(x + i*40), uint8(y + i*20), uint8((x*y)>>4 + i*60), 255})
			}
		}
		var buf bytes.Buffer
		if err := png.Encode(&buf, img); err != nil {
			t.Fatal(err)
		}
		bases[i] = buf.Bytes()
	}
	for i := 0; i < count; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("img%04d.png", i)), bases[i%4], 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// genMediaCopies generates 4 distinct media files with ffmpeg (from the given
// lavfi source specs), then copies them to reach `count` files sharing duration.
func genMediaCopies(t *testing.T, dir string, count int, ext string, specs []string, argv func(spec, out string) []string) {
	t.Helper()
	if !videoToolsAvailable() {
		t.Skipf("ffmpeg required to generate synthetic %s files", ext)
	}
	scratch := t.TempDir()
	seeds := make([][]byte, len(specs))
	for i, spec := range specs {
		p := filepath.Join(scratch, fmt.Sprintf("s%d.%s", i, ext))
		cmd := exec.Command("ffmpeg", append([]string{"-hide_banner", "-loglevel", "error"}, argv(spec, p)...)...)
		if err := cmd.Run(); err != nil {
			t.Fatalf("ffmpeg gen %q: %v", spec, err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		seeds[i] = b
	}
	for i := 0; i < count; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("m%04d.%s", i, ext)), seeds[i%len(seeds)], 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
