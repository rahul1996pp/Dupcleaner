package dup

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCollectFiles guards the filepath.WalkDir traversal: correct extension
// filtering, hidden-file skipping, exclude-dir pruning, and min-size filtering.
func TestCollectFiles(t *testing.T) {
	root := t.TempDir()
	write := func(rel string, size int) string {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, size), 0644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	keep := write("a.jpg", 2048)        // matches
	write("b.txt", 2048)                // wrong extension
	write("small.jpg", 10)              // below min size
	keepSub := write("sub/c.jpg", 2048) // matches, nested
	write("excluded/d.jpg", 2048)       // pruned by exclude dir
	write(".hidden.jpg", 2048)          // hidden file skipped

	exts := map[string]bool{".jpg": true}
	exclude := []string{filepath.Join(root, "excluded")}
	got := collectFiles([]string{root}, true /*skipHidden*/, 1 /*minSizeKB*/, exts, exclude)

	gotSet := make(map[string]bool, len(got))
	for _, f := range got {
		gotSet[f.Path] = true
	}
	for _, w := range []string{keep, keepSub} {
		abs, _ := filepath.Abs(w)
		if !gotSet[abs] {
			t.Errorf("expected %s to be collected", w)
		}
	}
	if len(got) != 2 {
		t.Errorf("expected exactly 2 files, got %d: %v", len(got), got)
	}
}
