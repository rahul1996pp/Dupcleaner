package dup

import (
	"testing"
)

func TestParseFpcalcJSON(t *testing.T) {
	data := []byte(`{"duration": 123.45, "fingerprint": [1, 2, 3, 4]}`)
	dur, fp, err := parseFpcalcJSON(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dur != 123.45 {
		t.Errorf("duration = %v, want 123.45", dur)
	}
	if len(fp) != 4 {
		t.Fatalf("fingerprint len = %d, want 4", len(fp))
	}
	if fp[0] != 1 || fp[3] != 4 {
		t.Errorf("fingerprint = %v, want [1 2 3 4]", fp)
	}
}

func TestParseFpcalcJSON_NoDuration(t *testing.T) {
	data := []byte(`{"fingerprint": [1, 2, 3]}`)
	if _, _, err := parseFpcalcJSON(data); err == nil {
		t.Error("expected error when duration missing")
	}
}

func TestParseFpcalcJSON_EmptyFingerprint(t *testing.T) {
	data := []byte(`{"duration": 100, "fingerprint": []}`)
	if _, _, err := parseFpcalcJSON(data); err == nil {
		t.Error("expected error when fingerprint empty")
	}
}

func TestAudioFingerprintSimilarity_Identical(t *testing.T) {
	a := []uint32{0xDEADBEEF, 0x12345678, 0xFFFFFFFF}
	if s := audioFingerprintSimilarity(a, a); s != 1.0 {
		t.Errorf("similarity = %v, want 1.0", s)
	}
}

func TestAudioFingerprintSimilarity_Orthogonal(t *testing.T) {
	a := []uint32{0x00000000, 0xFFFFFFFF}
	b := []uint32{0xFFFFFFFF, 0x00000000}
	if s := audioFingerprintSimilarity(a, b); s != 0.0 {
		t.Errorf("similarity = %v, want 0.0 (all bits flipped)", s)
	}
}

func TestAudioFingerprintSimilarity_Empty(t *testing.T) {
	if s := audioFingerprintSimilarity(nil, []uint32{1, 2, 3}); s != 0.0 {
		t.Errorf("similarity = %v, want 0.0 (empty)", s)
	}
}

func TestAudioFingerprintSimilarity_DifferentLengths(t *testing.T) {
	a := []uint32{1, 2, 3}
	b := []uint32{1, 2}
	// Overlap (first 2 words) is identical → 1.0
	if s := audioFingerprintSimilarity(a, b); s != 1.0 {
		t.Errorf("similarity = %v, want 1.0 (identical overlap)", s)
	}
}

func TestGroupAudiosByFingerprint_BasicDup(t *testing.T) {
	a := &ImageInfo{Path: "a", Duration: 100, AHashes: []uint32{1, 2, 3, 4}}
	b := &ImageInfo{Path: "b", Duration: 100.3, AHashes: []uint32{1, 2, 3, 4}}
	groups := groupAudiosByFingerprint([]*ImageInfo{a, b}, 0.8)
	if len(groups) != 1 {
		t.Fatalf("got %d groups, want 1", len(groups))
	}
	if len(groups[0]) != 2 {
		t.Fatalf("group size %d, want 2", len(groups[0]))
	}
}

func TestGroupAudiosByFingerprint_DurationGate(t *testing.T) {
	// Identical fingerprints but far-apart durations must NOT group.
	a := &ImageInfo{Path: "a", Duration: 100, AHashes: []uint32{1, 2, 3, 4}}
	b := &ImageInfo{Path: "b", Duration: 400, AHashes: []uint32{1, 2, 3, 4}}
	groups := groupAudiosByFingerprint([]*ImageInfo{a, b}, 0.8)
	if len(groups) != 0 {
		t.Fatalf("got %d groups, want 0 (duration gate)", len(groups))
	}
}

func TestGroupAudiosByFingerprint_BelowThreshold(t *testing.T) {
	// Half the bits flipped → similarity ~0.5, below threshold 0.8.
	a := &ImageInfo{Path: "a", Duration: 100, AHashes: []uint32{0x0000FFFF, 0x0000FFFF}}
	b := &ImageInfo{Path: "b", Duration: 100.2, AHashes: []uint32{0xFFFFFFFF, 0xFFFFFFFF}}
	groups := groupAudiosByFingerprint([]*ImageInfo{a, b}, 0.8)
	if len(groups) != 0 {
		t.Fatalf("got %d groups, want 0 (below threshold)", len(groups))
	}
}

func TestFpcalcBin_Default(t *testing.T) {
	toolsCfgMu.Lock()
	prev := toolsCfg.FPcalc
	toolsCfg.FPcalc = ""
	toolsCfgMu.Unlock()
	defer func() {
		toolsCfgMu.Lock()
		toolsCfg.FPcalc = prev
		toolsCfgMu.Unlock()
	}()
	if got := fpcalcBin(); got != "fpcalc" {
		t.Errorf("fpcalcBin() = %q, want \"fpcalc\"", got)
	}
}
