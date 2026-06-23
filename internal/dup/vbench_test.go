package dup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestPacketBench benchmarks the new compressed-domain packet signature against
// the existing perceptual keyframe pHash across a corpus of video variants.
// It is skipped unless DUP_BENCH_DIR points at a folder of test videos, so it
// never runs in normal `go test`. Run it with:
//
//	DUP_BENCH_DIR=/tmp go test -run TestPacketBench -v -timeout 20m
//
// The corpus should contain one baseline named with a leading "A" plus its
// variants (A_copy, A_remux, A_crf30, ...) and at least one unrelated "B" video.
func TestPacketBench(t *testing.T) {
	dir := os.Getenv("DUP_BENCH_DIR")
	if dir == "" {
		t.Skip("set DUP_BENCH_DIR=<folder of test videos> to run the benchmark")
	}
	if !videoToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not available")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read corpus dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if videoExts[strings.ToLower(filepath.Ext(e.Name()))] {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	if len(files) < 2 {
		t.Fatalf("need >=2 videos in %s, found %d", dir, len(files))
	}

	ctx := context.Background()
	type rec struct {
		name     string
		dur      float64
		w, h     int
		psig     []float32
		vh       []uint64
		tPacket  time.Duration
		tFrames  time.Duration
		okPacket bool
		okFrames bool
	}
	var recs []rec
	var sumPacket, sumFrames time.Duration

	for _, f := range files {
		dur, w, h, _ := probeVideo(ctx, f)
		t0 := time.Now()
		psig, okP := packetFingerprint(ctx, f)
		tP := time.Since(t0)
		t1 := time.Now()
		vh := fingerprintVideo(ctx, f, dur)
		tF := time.Since(t1)
		sumPacket += tP
		sumFrames += tF
		recs = append(recs, rec{
			name: filepath.Base(f), dur: dur, w: w, h: h,
			psig: psig, vh: vh, tPacket: tP, tFrames: tF,
			okPacket: okP, okFrames: len(vh) > 0,
		})
	}

	// ── Timing table ──
	t.Logf("")
	t.Logf("════════════ PER-FILE TIMING (wall-clock per fingerprint) ════════════")
	t.Logf("%-18s %7s %10s %12s %12s %9s", "file", "dur(s)", "res", "packet", "frames(8)", "speedup")
	for _, r := range recs {
		sp := 0.0
		if r.tPacket > 0 {
			sp = float64(r.tFrames) / float64(r.tPacket)
		}
		t.Logf("%-18s %7.1f %10s %12s %12s %8.1fx",
			r.name, r.dur, fmt.Sprintf("%dx%d", r.w, r.h),
			r.tPacket.Round(time.Millisecond), r.tFrames.Round(time.Millisecond), sp)
	}
	overall := 0.0
	if sumPacket > 0 {
		overall = float64(sumFrames) / float64(sumPacket)
	}
	t.Logf("%-18s %7s %10s %12s %12s %8.1fx", "TOTAL", "", "",
		sumPacket.Round(time.Millisecond), sumFrames.Round(time.Millisecond), overall)

	// ── Detection analysis vs baseline (the "A" video) ──
	base := 0
	for i, r := range recs {
		if strings.EqualFold(r.name, "A.mp4") {
			base = i
		}
	}
	const packetMatch = 0.99 // cosine ≥ this ⇒ "same encode"
	const frameMatch = 0.88  // 1-dist/64 ≥ this ⇒ "visually same"
	t.Logf("")
	t.Logf("════════════ DETECTION vs baseline %q ════════════", recs[base].name)
	t.Logf("%-18s %12s %12s   %s", "file", "packet-cos", "frame-sim", "verdict")
	for i, r := range recs {
		pc := packetSignatureSimilarity(recs[base].psig, r.psig)
		fs := 0.0
		if len(recs[base].vh) > 0 && len(r.vh) > 0 {
			fs = 1.0 - float64(avgFrameDistance(recs[base].vh, r.vh))/64.0
		}
		verdict := "—"
		switch {
		case i == base:
			verdict = "(baseline)"
		case pc >= packetMatch:
			verdict = "DUP via packet (no decode)"
		case fs >= frameMatch:
			verdict = "DUP via frame pHash"
		default:
			verdict = "not matched"
		}
		t.Logf("%-18s %12.4f %12.4f   %s", r.name, pc, fs, verdict)
	}
	t.Logf("")
	t.Logf("packet-match threshold=%.2f  frame-match threshold=%.2f", packetMatch, frameMatch)
}

// TestCascade runs the FULL video pipeline (produceVideoInfos + groupVideoDuplicates)
// on the corpus to verify the conservative no-decode tier groups copies/remuxes
// correctly and keeps unrelated content (B) in a separate group. The "No-decode
// tier: N candidate(s)..." log line (visible with -v) shows how many frame decodes
// were skipped. Skipped unless DUP_BENCH_DIR is set.
func TestCascade(t *testing.T) {
	dir := os.Getenv("DUP_BENCH_DIR")
	if dir == "" {
		t.Skip("set DUP_BENCH_DIR=<folder of test videos> to run the cascade test")
	}
	if !videoToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not available")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read corpus dir: %v", err)
	}
	var files []*FileEntry
	for _, e := range entries {
		if e.IsDir() || !videoExts[strings.ToLower(filepath.Ext(e.Name()))] {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, &FileEntry{
			Path: filepath.Join(dir, e.Name()), Size: fi.Size(), ModTime: fi.ModTime(),
		})
	}

	req := ScanRequest{Category: "videos", Mode: "visual", UseCache: false, Threads: 4}
	images := produceVideoInfos(context.Background(), files, req, 4)
	groups := groupVideoDuplicates(images, 8) // maxAvgDist 8 ≈ "standard"

	t.Logf("")
	t.Logf("════════════ CASCADE GROUPING (maxAvgDist=8) ════════════")
	if len(groups) == 0 {
		t.Logf("no duplicate groups formed")
	}
	for _, g := range groups {
		names := make([]string, 0, len(g.Images))
		for _, im := range g.Images {
			names = append(names, im.Name)
		}
		sort.Strings(names)
		t.Logf("group %d (sim %.3f): %s", g.ID, g.Similarity, strings.Join(names, ", "))
	}
	t.Logf("")
	t.Logf("EXPECT: A.mp4 + A_copy.mp4 + A_remux.mkv together (no-decode), re-encodes joining via frame-hash, and B.mp4 + B_copy.mp4 in a SEPARATE group.")
}
