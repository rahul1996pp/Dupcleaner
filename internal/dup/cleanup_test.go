package dup

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRuleSaveLoadRoundTrip(t *testing.T) {
	t.Chdir(t.TempDir())
	rules.Rules = []CleanupRule{
		{Name: "big", Strategy: "largest", MinWastedMB: 5},
		{Name: "fav", Strategy: "prefer_dir", PreferDir: "/keep"},
	}
	if err := saveRules(); err != nil {
		t.Fatalf("saveRules: %v", err)
	}
	rules.Rules = nil
	loadRules()
	if len(rules.Rules) != 2 {
		t.Fatalf("len = %d, want 2", len(rules.Rules))
	}
	if rules.Rules[0].Name != "big" || rules.Rules[0].Strategy != "largest" || rules.Rules[0].MinWastedMB != 5 {
		t.Errorf("rule 0 not preserved: %+v", rules.Rules[0])
	}
	if rules.Rules[1].Name != "fav" || rules.Rules[1].Strategy != "prefer_dir" || rules.Rules[1].PreferDir != "/keep" {
		t.Errorf("rule 1 not preserved: %+v", rules.Rules[1])
	}
}

func TestRuleLoadMissingFile(t *testing.T) {
	t.Chdir(t.TempDir())
	rules.Rules = nil
	loadRules() // must not panic
	if len(rules.Rules) != 0 {
		t.Errorf("len = %d, want 0", len(rules.Rules))
	}
}

func TestApplyStrategyMatchesSmartSelect(t *testing.T) {
	old := time.Now().Add(-48 * time.Hour)
	mid := time.Now().Add(-24 * time.Hour)

	resGroup := &DuplicateGroup{Images: []*ImageInfo{
		{Path: "lo", Width: 640, Height: 480},
		{Path: "hi", Width: 1920, Height: 1080},
	}}
	timeGroup := &DuplicateGroup{Images: []*ImageInfo{
		{Path: "new", ModTime: mid},
		{Path: "old", ModTime: old},
	}}

	toDel, toKeep := applyStrategy([]*DuplicateGroup{resGroup}, "highest_res", "", 0)
	if len(toKeep) != 1 || toKeep[0] != "hi" {
		t.Errorf("highest_res keep = %v, want [hi]", toKeep)
	}
	if len(toDel) != 1 || toDel[0] != "lo" {
		t.Errorf("highest_res del = %v, want [lo]", toDel)
	}

	toDel, toKeep = applyStrategy([]*DuplicateGroup{timeGroup}, "oldest", "", 0)
	if len(toKeep) != 1 || toKeep[0] != "old" {
		t.Errorf("oldest keep = %v, want [old]", toKeep)
	}
	if len(toDel) != 1 || toDel[0] != "new" {
		t.Errorf("oldest del = %v, want [new]", toDel)
	}
}

func TestApplyStrategyMinWastedFilter(t *testing.T) {
	g := &DuplicateGroup{
		WastedSize: 1024 * 1024, // 1 MB
		Images: []*ImageInfo{
			{Path: "a", Size: 100},
			{Path: "b", Size: 200},
		},
	}
	toDel, _ := applyStrategy([]*DuplicateGroup{g}, "largest", "", 5)
	if len(toDel) != 0 {
		t.Errorf("min 5MB: del = %v, want none (group is 1MB)", toDel)
	}
	toDel, _ = applyStrategy([]*DuplicateGroup{g}, "largest", "", 0.5)
	if len(toDel) != 1 {
		t.Errorf("min 0.5MB: del = %v, want 1", toDel)
	}
}

func TestApplyStrategyPreferDir(t *testing.T) {
	g := &DuplicateGroup{Images: []*ImageInfo{
		{Path: "a", Dir: "/tmp/other"},
		{Path: "b", Dir: "/tmp/keep"},
	}}
	_, toKeep := applyStrategy([]*DuplicateGroup{g}, "prefer_dir", "/tmp/keep", 0)
	if len(toKeep) != 1 || toKeep[0] != "b" {
		t.Errorf("prefer_dir keep = %v, want [b]", toKeep)
	}
}

func TestApplyStrategyPreferDirBoundary(t *testing.T) {
	// A sibling dir that merely shares a name prefix with the preferred dir
	// must NOT be treated as "inside" it. pd "/tmp/keep" should not match
	// "/tmp/keepsafe"; with no real match the strategy falls back to the first.
	g := &DuplicateGroup{Images: []*ImageInfo{
		{Path: "a", Dir: "/tmp/other"},
		{Path: "b", Dir: "/tmp/keepsafe"},
	}}
	_, toKeep := applyStrategy([]*DuplicateGroup{g}, "prefer_dir", "/tmp/keep", 0)
	if len(toKeep) != 1 || toKeep[0] != "a" {
		t.Errorf("prefer_dir boundary keep = %v, want [a] (sibling prefix must not match)", toKeep)
	}
}

func TestApplyStrategyPreferDirSubdir(t *testing.T) {
	// A descendant of the preferred dir should match.
	g := &DuplicateGroup{Images: []*ImageInfo{
		{Path: "a", Dir: "/tmp/other"},
		{Path: "b", Dir: "/tmp/keep/sub"},
	}}
	_, toKeep := applyStrategy([]*DuplicateGroup{g}, "prefer_dir", "/tmp/keep", 0)
	if len(toKeep) != 1 || toKeep[0] != "b" {
		t.Errorf("prefer_dir subdir keep = %v, want [b]", toKeep)
	}
}

func TestValidStrategy(t *testing.T) {
	for _, s := range []string{"highest_res", "largest", "oldest", "newest", "prefer_dir"} {
		if !validStrategy(s) {
			t.Errorf("validStrategy(%q) = false, want true", s)
		}
	}
	if validStrategy("") {
		t.Error(`validStrategy("") = true, want false`)
	}
	if validStrategy("bogus") {
		t.Error(`validStrategy("bogus") = true, want false`)
	}
}

func TestHandleSaveRuleRejectsBadStrategy(t *testing.T) {
	body := bytes.NewBufferString(`{"name":"x","strategy":"bogus"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/rules/save", body)
	rec := httptest.NewRecorder()
	handleSaveRule(rec, req)
	if rec.Code != 400 {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleSaveRuleUpsert(t *testing.T) {
	t.Chdir(t.TempDir())
	rules.Rules = nil

	post := func(payload string) {
		req := httptest.NewRequest(http.MethodPost, "/api/rules/save", bytes.NewBufferString(payload))
		rec := httptest.NewRecorder()
		handleSaveRule(rec, req)
		if rec.Code != 200 {
			t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
		}
	}

	post(`{"name":"test","strategy":"oldest"}`)
	post(`{"name":"test","strategy":"newest"}`)

	if len(rules.Rules) != 1 {
		t.Fatalf("len = %d, want 1 (upsert by name)", len(rules.Rules))
	}
	if rules.Rules[0].Strategy != "newest" {
		t.Errorf("strategy = %q, want newest", rules.Rules[0].Strategy)
	}
}
