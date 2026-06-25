package dup

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeFileAt writes content to dir/name and forces its mtime, returning a
// FileEntry describing it as the scanner would see it.
func writeFileAt(t *testing.T, dir, name, content string, mt time.Time) *FileEntry {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return &FileEntry{Path: p, Size: st.Size(), ModTime: st.ModTime()}
}

// TestCacheGetByPath covers the primary lookup: an exact path match whose size
// and mtime agree is a hit; a size or mtime change (file edited) is a miss.
func TestCacheGetByPath(t *testing.T) {
	c := NewCache()
	mt := time.Unix(1700000000, 0)
	c.Set("/a/file.mp4", &CacheEntry{Size: 100, ModUnix: mt.UnixNano(), MD5: "abc"})

	if e, ok := c.Get("/a/file.mp4", 100, mt); !ok || e.MD5 != "abc" {
		t.Fatalf("exact path hit failed: ok=%v entry=%+v", ok, e)
	}
	if _, ok := c.Get("/a/file.mp4", 999, mt); ok {
		t.Error("size change must invalidate (miss)")
	}
	if _, ok := c.Get("/a/file.mp4", 100, mt.Add(time.Second)); ok {
		t.Error("mtime change must invalidate (miss)")
	}
}

// TestCacheSurvivesRename is the fix: a file moved/renamed (same bytes, same
// size+mtime, new path) reuses its cached fingerprint instead of recomputing.
// Crucially it also pins the SAFETY property — a different file that merely
// shares a size and timestamp must NOT match, because the content is verified.
func TestCacheSurvivesRename(t *testing.T) {
	c := NewCache()
	dir := t.TempDir()
	mt := time.Unix(1700000000, 12345)
	const content = "the original video bytes — long enough to be meaningful"

	orig := writeFileAt(t, dir, "clip.mp4", content, mt)
	c.Set(orig.Path, &CacheEntry{
		Size: orig.Size, ModUnix: orig.ModTime.UnixNano(),
		Duration: 100, VHashes: []uint64{1, 2, 3}, HasVHashes: true,
		Quick: contentID(orig.Path, orig.Size), // content identity as the scanner would store
	})

	// Same bytes, new path, same size+mtime → must hit and reuse the fingerprint.
	moved := writeFileAt(t, dir, "renamed.mp4", content, mt)
	e, ok := c.Get(moved.Path, moved.Size, moved.ModTime)
	if !ok {
		t.Fatal("moved file with identical content should be a cache hit")
	}
	if !e.HasVHashes || len(e.VHashes) != 3 {
		t.Errorf("moved file should reuse cached fingerprint, got %+v", e)
	}

	// SAFETY: a DIFFERENT file engineered to share the exact size and mtime must
	// NOT match — this is the case (archive extracts, cp -p) that a naive
	// (size,mtime) cache would mis-group, risking deletion of a non-duplicate.
	diff := writeFileAt(t, dir, "different.mp4", "XXX original video bytes — long enough to be meaningful", mt)
	if diff.Size != orig.Size {
		t.Fatalf("test setup: decoy size %d != original %d", diff.Size, orig.Size)
	}
	if _, ok := c.Get(diff.Path, diff.Size, diff.ModTime); ok {
		t.Error("different content with same size+mtime must NOT be a cache hit")
	}

	// A genuinely different size under a new path has no candidate at all → miss.
	if _, ok := c.Get(filepath.Join(dir, "other.mp4"), orig.Size+10, mt); ok {
		t.Error("different size must not match the rename index")
	}
}

// TestCacheRenameNeedsStoredIdentity verifies that a candidate WITHOUT a stored
// content identity (e.g. an old cache entry from before this feature) is never
// reused across paths — it safely falls back to recomputing.
func TestCacheRenameNeedsStoredIdentity(t *testing.T) {
	c := NewCache()
	dir := t.TempDir()
	mt := time.Unix(1700000000, 0)
	orig := writeFileAt(t, dir, "a.mp4", "some bytes here", mt)
	// Stored entry has NO Quick (Quick == "").
	c.Set(orig.Path, &CacheEntry{Size: orig.Size, ModUnix: orig.ModTime.UnixNano(),
		VHashes: []uint64{9}, HasVHashes: true})

	moved := writeFileAt(t, dir, "b.mp4", "some bytes here", mt)
	if _, ok := c.Get(moved.Path, moved.Size, moved.ModTime); ok {
		t.Error("entry without a stored content identity must not be reused across paths")
	}
}

// TestCacheLoadRebuildsRenameIndex verifies the (size,mtime) index — which is
// NOT serialized — is rebuilt on Load, so rename resilience survives a restart.
func TestCacheLoadRebuildsRenameIndex(t *testing.T) {
	dir := t.TempDir()
	mt := time.Unix(1700000000, 777)
	const content = "persisted content bytes for round-trip"
	orig := writeFileAt(t, dir, "orig.mp4", content, mt)

	c1 := NewCache()
	c1.Set(orig.Path, &CacheEntry{
		Size: orig.Size, ModUnix: orig.ModTime.UnixNano(),
		Duration: 50, VHashes: []uint64{7, 8}, HasVHashes: true,
		Quick: contentID(orig.Path, orig.Size),
	})
	cachePath := filepath.Join(dir, "cache.json")
	if err := c1.Save(cachePath); err != nil {
		t.Fatal(err)
	}

	// Fresh cache loaded from disk — bySig must be reconstructed from Entries.
	c2 := NewCache()
	if err := c2.Load(cachePath); err != nil {
		t.Fatal(err)
	}
	moved := writeFileAt(t, dir, "moved.mp4", content, mt)
	if _, ok := c2.Get(moved.Path, moved.Size, moved.ModTime); !ok {
		t.Error("rename resilience must survive Save/Load (index rebuilt on Load)")
	}
}

// TestCacheRenameIndexAfterClear verifies the secondary index is reset by Clear.
func TestCacheRenameIndexAfterClear(t *testing.T) {
	c := NewCache()
	mt := time.Unix(1700000000, 0)
	c.Set("/a/x.mp4", &CacheEntry{Size: 10, ModUnix: mt.UnixNano(), MD5: "z", Quick: "z"})
	c.Clear()
	if _, ok := c.Get("/b/x.mp4", 10, mt); ok {
		t.Error("rename index must be empty after Clear")
	}
}

// TestJournalRecoversInterruptedScan is the durability fix: entries Set after
// OpenJournal are written to the journal immediately, so a process that dies
// before it can compact (no Save) still has its work recovered by the next run.
func TestJournalRecoversInterruptedScan(t *testing.T) {
	jpath := filepath.Join(t.TempDir(), "cache.log")
	mt := time.Unix(1700000000, 0)

	// Run 1: journaling on, two entries computed, then "crash" (no Save/Close).
	c1 := NewCache()
	if _, err := c1.OpenJournal(jpath); err != nil {
		t.Fatal(err)
	}
	c1.Set("/a/clip.mp4", &CacheEntry{Size: 100, ModUnix: mt.UnixNano(), MD5: "aaa", HasVHashes: true, VHashes: []uint64{1, 2}})
	c1.Set("/b/song.mp3", &CacheEntry{Size: 200, ModUnix: mt.UnixNano(), HasAHashes: true, AHashes: []uint32{9}})
	c1.CloseJournal() // release the handle; the log on disk is exactly a crash's state

	// Run 2: a fresh cache (empty snapshot) replays the journal on open.
	c2 := NewCache()
	recovered, err := c2.OpenJournal(jpath)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.CloseJournal()
	if recovered != 2 {
		t.Fatalf("expected to recover 2 entries from journal, got %d", recovered)
	}
	if e, ok := c2.Get("/a/clip.mp4", 100, mt); !ok || !e.HasVHashes || len(e.VHashes) != 2 {
		t.Errorf("video entry not recovered: ok=%v entry=%+v", ok, e)
	}
	if e, ok := c2.Get("/b/song.mp3", 200, mt); !ok || !e.HasAHashes {
		t.Errorf("audio entry not recovered: ok=%v entry=%+v", ok, e)
	}
}

// TestJournalCompactsOnSave verifies Save folds the journal into the snapshot and
// truncates the log, so it does not grow without bound across scans.
func TestJournalCompactsOnSave(t *testing.T) {
	dir := t.TempDir()
	jpath := filepath.Join(dir, "cache.log")
	spath := filepath.Join(dir, "cache.json")
	mt := time.Unix(1700000000, 0)

	c := NewCache()
	if _, err := c.OpenJournal(jpath); err != nil {
		t.Fatal(err)
	}
	defer c.CloseJournal()
	c.Set("/a/x.jpg", &CacheEntry{Size: 10, ModUnix: mt.UnixNano(), MD5: "x"})
	beforeStat, _ := os.Stat(jpath)
	if err := c.Save(spath); err != nil {
		t.Fatal(err)
	}
	afterStat, err := os.Stat(jpath)
	if err != nil {
		t.Fatal(err)
	}
	if afterStat.Size() >= beforeStat.Size() {
		t.Errorf("journal should shrink after compaction: before=%d after=%d", beforeStat.Size(), afterStat.Size())
	}
	// The snapshot must now hold the entry, and a reload must still find it.
	c2 := NewCache()
	if err := c2.Load(spath); err != nil {
		t.Fatal(err)
	}
	if _, ok := c2.Get("/a/x.jpg", 10, mt); !ok {
		t.Error("snapshot must contain the entry after compaction")
	}
}

// TestJournalSkipsTornLine verifies a malformed/torn final line (the classic
// crash-mid-write artifact) is skipped, while valid records before it still load.
func TestJournalSkipsTornLine(t *testing.T) {
	jpath := filepath.Join(t.TempDir(), "cache.log")
	mt := time.Unix(1700000000, 0)
	// One good record (m is UnixNano), then a half-written line cut off by a "crash".
	good := `{"p":"/a/x.jpg","e":{"s":10,"m":1700000000000000000,"md5":"x"}}`
	data := good + "\n" + `{"p":"/a/y.jpg","e":{"s":20,"m":17000` // truncated, no newline
	if err := os.WriteFile(jpath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	c := NewCache()
	recovered, err := c.OpenJournal(jpath)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseJournal()
	if recovered != 1 {
		t.Errorf("expected 1 valid record recovered (torn line skipped), got %d", recovered)
	}
	if _, ok := c.Get("/a/x.jpg", 10, mt); !ok {
		t.Error("the valid record before the torn line must load")
	}
}
