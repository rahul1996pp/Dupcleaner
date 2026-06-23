package dup

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// writeTestJPEG writes a w×h JPEG to path for use as thumbnail-decode input.
func writeTestJPEG(t *testing.T, path string, w, h int) {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.NRGBA{R: uint8(x % 256), G: uint8(y % 256), B: 128, A: 255})
		}
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	if err := jpeg.Encode(f, img, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatalf("encode: %v", err)
	}
}

func TestEncodeImageThumb(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.jpg")
	writeTestJPEG(t, src, 800, 600)

	data, err := encodeImageThumb(src)
	if err != nil {
		t.Fatalf("encodeImageThumb: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty thumbnail bytes")
	}
	img, err := jpeg.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode thumb: %v", err)
	}
	if b := img.Bounds(); b.Dx() > thumbStdSize || b.Dy() > thumbStdSize {
		t.Errorf("thumb %dx%d exceeds %d", b.Dx(), b.Dy(), thumbStdSize)
	}
}

func TestEncodeImageThumb_BadFile(t *testing.T) {
	if _, err := encodeImageThumb(filepath.Join(t.TempDir(), "nope.jpg")); err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestWriteThumbAtomic(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "ab", "cd.jpg")
	if err := writeThumbAtomic(dst, []byte("hello")); err != nil {
		t.Fatalf("writeThumbAtomic: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("content = %q, want hello", got)
	}
	if _, statErr := os.Stat(dst + ".tmp"); statErr == nil {
		t.Error("temp file should not remain")
	}
}

func TestServeCachedThumb(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "video.mp4")
	if err := os.WriteFile(src, []byte("not a real video"), 0644); err != nil {
		t.Fatal(err)
	}
	stat, _ := os.Stat(src)
	cachePath := thumbCachePath(src, stat.Size(), stat.ModTime().UnixNano())
	if err := writeThumbAtomic(cachePath, []byte("JPEGDATA")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(filepath.Dir(cachePath)) })

	// Poster present → served as image/jpeg with our exact bytes.
	rec := httptest.NewRecorder()
	if !serveCachedThumb(rec, httptest.NewRequest("GET", "/api/thumbnail?path="+src, nil), src) {
		t.Fatal("expected serveCachedThumb to serve")
	}
	if ct := rec.Header().Get("Content-Type"); ct != "image/jpeg" {
		t.Errorf("content-type = %q, want image/jpeg", ct)
	}
	if rec.Body.String() != "JPEGDATA" {
		t.Errorf("body = %q, want JPEGDATA", rec.Body.String())
	}

	// No poster cached → returns false (caller will serve an icon).
	src2 := filepath.Join(dir, "other.mp4")
	if err := os.WriteFile(src2, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	rec2 := httptest.NewRecorder()
	if serveCachedThumb(rec2, httptest.NewRequest("GET", "/x", nil), src2) {
		t.Error("expected false when no poster cached")
	}
}

// setGroups replaces scan results for a test and restores them afterward.
func setGroups(t *testing.T, groups []*DuplicateGroup) {
	t.Helper()
	state.mu.Lock()
	prev := state.groups
	state.groups = groups
	state.mu.Unlock()
	t.Cleanup(func() {
		state.mu.Lock()
		state.groups = prev
		state.mu.Unlock()
	})
}

func TestThumbConcurrency(t *testing.T) {
	w, vt := thumbConcurrency()
	if w < 1 || w > 8 {
		t.Errorf("workers = %d, want 1..8", w)
	}
	if vt != 1 && vt != 2 {
		t.Errorf("videoThreads = %d, want 1 or 2", vt)
	}
}

func TestCollectThumbTargets(t *testing.T) {
	groups := []*DuplicateGroup{
		{Images: []*ImageInfo{
			{Path: "/a/one.jpg"},
			{Path: "/a/clip.mp4"},
			{Path: "/a/song.mp3"}, // audio — excluded
			{Path: "/a/doc.pdf"},  // doc — excluded
			{Path: "/a/one.jpg"},  // duplicate path — deduped
		}},
		{Images: []*ImageInfo{{Path: "/b/two.png"}}},
	}
	got := collectThumbTargets(groups)
	if len(got) != 3 {
		t.Fatalf("targets = %d, want 3 (two images, one video)", len(got))
	}
	vid := 0
	for _, tg := range got {
		if tg.isVideo {
			vid++
			if tg.path != "/a/clip.mp4" {
				t.Errorf("video target = %s, want /a/clip.mp4", tg.path)
			}
		}
	}
	if vid != 1 {
		t.Errorf("video targets = %d, want 1", vid)
	}
}

func TestRunThumbGen_Images(t *testing.T) {
	atomic.StoreInt64(&cancelThumb, 0)
	dir := t.TempDir()
	a := filepath.Join(dir, "a.jpg")
	b := filepath.Join(dir, "b.jpg")
	writeTestJPEG(t, a, 320, 240)
	writeTestJPEG(t, b, 320, 240)
	setGroups(t, []*DuplicateGroup{{Images: []*ImageInfo{{Path: a}, {Path: b}}}})
	t.Cleanup(func() { os.RemoveAll("thumbs") })

	runThumbGen() // blocks until complete

	state.mu.RLock()
	p := state.thumbProgress
	state.mu.RUnlock()
	if !p.Complete || p.Running {
		t.Errorf("progress = %+v, want Complete && !Running", p)
	}
	if p.Done != 2 || p.Failed != 0 {
		t.Errorf("Done=%d Failed=%d, want 2/0", p.Done, p.Failed)
	}
	for _, src := range []string{a, b} {
		st, _ := os.Stat(src)
		if _, err := os.Stat(thumbCachePath(src, st.Size(), st.ModTime().UnixNano())); err != nil {
			t.Errorf("missing cached thumb for %s: %v", src, err)
		}
	}
}

func TestRunThumbGen_SkipsCached(t *testing.T) {
	atomic.StoreInt64(&cancelThumb, 0)
	dir := t.TempDir()
	a := filepath.Join(dir, "a.jpg")
	writeTestJPEG(t, a, 320, 240)
	st, _ := os.Stat(a)
	dst := thumbCachePath(a, st.Size(), st.ModTime().UnixNano())
	if err := writeThumbAtomic(dst, []byte("SENTINEL")); err != nil {
		t.Fatal(err)
	}
	setGroups(t, []*DuplicateGroup{{Images: []*ImageInfo{{Path: a}}}})
	t.Cleanup(func() { os.RemoveAll("thumbs") })

	runThumbGen()

	got, _ := os.ReadFile(dst)
	if string(got) != "SENTINEL" {
		t.Error("cached thumbnail was regenerated; expected skip")
	}
	state.mu.RLock()
	p := state.thumbProgress
	state.mu.RUnlock()
	if p.Done != 1 || p.Failed != 0 {
		t.Errorf("Done=%d Failed=%d, want 1/0", p.Done, p.Failed)
	}
}

func TestRunThumbGen_Cancel(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.jpg")
	writeTestJPEG(t, a, 320, 240)
	setGroups(t, []*DuplicateGroup{{Images: []*ImageInfo{{Path: a}}}})
	t.Cleanup(func() {
		atomic.StoreInt64(&cancelThumb, 0)
		os.RemoveAll("thumbs")
	})

	atomic.StoreInt64(&cancelThumb, 1) // pre-set: job must honor it immediately
	runThumbGen()

	state.mu.RLock()
	p := state.thumbProgress
	state.mu.RUnlock()
	if p.Status != "Cancelled" {
		t.Errorf("status = %q, want Cancelled", p.Status)
	}
	if p.Done != 0 {
		t.Errorf("Done = %d, want 0 (cancelled before dispatch)", p.Done)
	}
}

func TestHandleThumbGenStart_NoResults(t *testing.T) {
	setGroups(t, nil)
	rec := httptest.NewRecorder()
	handleThumbGenStart(rec, httptest.NewRequest("POST", "/api/thumbnails/generate", nil))
	if rec.Code != 400 {
		t.Errorf("code = %d, want 400 (no results)", rec.Code)
	}
}

func TestHandleThumbGenStart_Busy(t *testing.T) {
	setGroups(t, []*DuplicateGroup{{Images: []*ImageInfo{{Path: "/x/a.jpg"}}}})
	state.mu.Lock()
	state.thumbing = true
	state.mu.Unlock()
	t.Cleanup(func() {
		state.mu.Lock()
		state.thumbing = false
		state.mu.Unlock()
	})

	rec := httptest.NewRecorder()
	handleThumbGenStart(rec, httptest.NewRequest("POST", "/api/thumbnails/generate", nil))
	if rec.Code != 409 {
		t.Errorf("code = %d, want 409 (busy)", rec.Code)
	}
}

func TestHandleThumbGenStart_Started(t *testing.T) {
	atomic.StoreInt64(&cancelThumb, 0)
	dir := t.TempDir()
	a := filepath.Join(dir, "a.jpg")
	writeTestJPEG(t, a, 200, 150)
	setGroups(t, []*DuplicateGroup{{Images: []*ImageInfo{{Path: a}}}})
	t.Cleanup(func() { os.RemoveAll("thumbs") })

	rec := httptest.NewRecorder()
	handleThumbGenStart(rec, httptest.NewRequest("POST", "/api/thumbnails/generate", nil))
	if rec.Code != 200 {
		t.Fatalf("code = %d, want 200", rec.Code)
	}

	// Wait for the spawned job to finish so it doesn't leak into other tests.
	deadline := time.Now().Add(5 * time.Second)
	for {
		state.mu.RLock()
		done := state.thumbProgress.Complete
		state.mu.RUnlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("generation did not complete within 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	state.mu.RLock()
	d := state.thumbProgress.Done
	state.mu.RUnlock()
	if d != 1 {
		t.Errorf("Done = %d, want 1", d)
	}
}

func TestHandleScan_RefusedWhileThumbing(t *testing.T) {
	state.mu.Lock()
	state.thumbing = true
	state.mu.Unlock()
	t.Cleanup(func() {
		state.mu.Lock()
		state.thumbing = false
		state.mu.Unlock()
	})

	body := `{"dirs":["` + filepath.ToSlash(t.TempDir()) + `"],"mode":"quick"}`
	rec := httptest.NewRecorder()
	handleScan(rec, httptest.NewRequest("POST", "/api/scan", strings.NewReader(body)))
	if rec.Code != 409 {
		t.Errorf("code = %d, want 409 (thumbnail gen in progress)", rec.Code)
	}
}
