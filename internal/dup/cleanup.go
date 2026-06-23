package dup

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// dirUnderPrefer reports whether dir is the preferred directory pd or a
// descendant of it. Paths are cleaned, and compared case-insensitively on
// Windows where the filesystem is case-insensitive. A plain HasPrefix would
// both miss case differences and spuriously match sibling dirs that merely
// share a name prefix (e.g. "C:\Photo" vs "C:\PhotoBackup").
func dirUnderPrefer(dir, pd string) bool {
	d := filepath.Clean(dir)
	p := filepath.Clean(pd)
	if runtime.GOOS == "windows" {
		d = strings.ToLower(d)
		p = strings.ToLower(p)
	}
	return d == p || strings.HasPrefix(d, p+string(filepath.Separator))
}

const rulesFile = "rules.json"

// CleanupRule is a saved auto-selection rule. It does NOT delete: applying a
// rule returns the same to_delete/to_keep shape as /api/smart-select.
type CleanupRule struct {
	Name        string  `json:"name"`
	Strategy    string  `json:"strategy"`
	PreferDir   string  `json:"prefer_dir,omitempty"`
	MinWastedMB float64 `json:"min_wasted_mb,omitempty"`
}

type rulesStore struct {
	mu    sync.RWMutex
	Rules []CleanupRule `json:"rules"`
}

var rules = &rulesStore{}

// rulesSaveMu serializes the marshal+write in saveRules so two concurrent
// handler calls (e.g. save and delete from different tabs) can't interleave
// os.WriteFile and corrupt rules.json.
var rulesSaveMu sync.Mutex

// loadRules reads rules.json if present. Missing file is not an error.
func loadRules() {
	data, err := os.ReadFile(rulesFile)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		logf("WARN", "rules.json read failed (ignoring): %v", err)
		return
	}
	var c struct {
		Rules []CleanupRule `json:"rules"`
	}
	if err := json.Unmarshal(data, &c); err != nil {
		logf("WARN", "rules.json parse failed (ignoring): %v", err)
		return
	}
	rules.mu.Lock()
	rules.Rules = c.Rules
	rules.mu.Unlock()
	logf("INFO", "Loaded %d cleanup rule(s)", len(c.Rules))
}

// saveRules persists the current rules to rules.json.
func saveRules() error {
	rulesSaveMu.Lock()
	defer rulesSaveMu.Unlock()
	rules.mu.RLock()
	data, err := json.MarshalIndent(struct {
		Rules []CleanupRule `json:"rules"`
	}{Rules: rules.Rules}, "", "  ")
	rules.mu.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(rulesFile, data, 0644)
}

// validStrategy reports whether s is one of the supported selection strategies.
func validStrategy(s string) bool {
	switch s {
	case "highest_res", "largest", "oldest", "newest", "prefer_dir":
		return true
	default:
		return false
	}
}

// applyStrategy runs a selection strategy over groups and returns the paths to
// delete and to keep. It auto-SELECTS only; it never deletes. minWastedMB > 0
// skips groups whose wasted size is below the threshold. This is the shared
// selection core used by both handleSmartSelect and handleApplyRule.
func applyStrategy(groups []*DuplicateGroup, strategy, preferDir string, minWastedMB float64) (toDel, toKeep []string) {
	toDel = make([]string, 0)
	toKeep = make([]string, 0)

	for _, g := range groups {
		if len(g.Images) < 2 {
			continue
		}
		if minWastedMB > 0 && float64(g.WastedSize)/1024/1024 < minWastedMB {
			continue
		}
		keepIdx := 0
		imgs := g.Images
		switch strategy {
		case "highest_res":
			for i, img := range imgs {
				if img.Width*img.Height > imgs[keepIdx].Width*imgs[keepIdx].Height {
					keepIdx = i
				}
			}
		case "largest":
			for i, img := range imgs {
				if img.Size > imgs[keepIdx].Size {
					keepIdx = i
				}
			}
		case "oldest":
			for i, img := range imgs {
				if img.ModTime.Before(imgs[keepIdx].ModTime) {
					keepIdx = i
				}
			}
		case "newest":
			for i, img := range imgs {
				if img.ModTime.After(imgs[keepIdx].ModTime) {
					keepIdx = i
				}
			}
		case "prefer_dir":
			pd := strings.TrimSpace(preferDir)
			if pd != "" {
				for i, img := range imgs {
					if dirUnderPrefer(img.Dir, pd) {
						keepIdx = i
						break
					}
				}
			}
		default:
			logf("WARN", "unknown strategy %q — defaulting to keep first", strategy)
		}
		toKeep = append(toKeep, imgs[keepIdx].Path)
		for i, img := range imgs {
			if i != keepIdx {
				toDel = append(toDel, img.Path)
			}
		}
	}
	return toDel, toKeep
}

// handleListRules returns the saved cleanup rules.
func handleListRules(w http.ResponseWriter, r *http.Request) {
	rules.mu.RLock()
	list := make([]CleanupRule, len(rules.Rules))
	copy(list, rules.Rules)
	rules.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"rules": list})
}

// handleSaveRule creates or updates (by Name) a cleanup rule.
func handleSaveRule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var rule CleanupRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if rule.Name == "" {
		http.Error(w, "name required", 400)
		return
	}
	if !validStrategy(rule.Strategy) {
		http.Error(w, "unknown strategy", 400)
		return
	}
	if rule.Strategy == "prefer_dir" && strings.TrimSpace(rule.PreferDir) == "" {
		http.Error(w, "prefer_dir strategy requires prefer_dir", 400)
		return
	}

	rules.mu.Lock()
	found := false
	for i := range rules.Rules {
		if rules.Rules[i].Name == rule.Name {
			rules.Rules[i] = rule
			found = true
			break
		}
	}
	if !found {
		rules.Rules = append(rules.Rules, rule)
	}
	rules.mu.Unlock()

	if err := saveRules(); err != nil {
		logf("WARN", "rules.json save failed: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"saved": true, "name": rule.Name})
}

// handleDeleteRule removes a cleanup rule by Name.
func handleDeleteRule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.Name == "" {
		http.Error(w, "name required", 400)
		return
	}

	rules.mu.Lock()
	kept := rules.Rules[:0]
	for _, rule := range rules.Rules {
		if rule.Name != req.Name {
			kept = append(kept, rule)
		}
	}
	rules.Rules = kept
	rules.mu.Unlock()

	if err := saveRules(); err != nil {
		logf("WARN", "rules.json save failed: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"deleted": true})
}

// handleApplyRule runs a saved rule over the current groups and returns the
// to_delete/to_keep selection (same shape as /api/smart-select). It never
// deletes.
func handleApplyRule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	rules.mu.RLock()
	var rule CleanupRule
	found := false
	for _, rl := range rules.Rules {
		if rl.Name == req.Name {
			rule = rl
			found = true
			break
		}
	}
	rules.mu.RUnlock()
	if !found {
		http.Error(w, "rule not found", 404)
		return
	}

	state.mu.RLock()
	groups := make([]*DuplicateGroup, len(state.groups))
	copy(groups, state.groups)
	state.mu.RUnlock()

	toDel, toKeep := applyStrategy(groups, rule.Strategy, rule.PreferDir, rule.MinWastedMB)
	logf("INFO", "Apply rule %q strategy=%s: %d to delete, %d to keep", rule.Name, rule.Strategy, len(toDel), len(toKeep))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"to_delete": toDel,
		"to_keep":   toKeep,
	})
}
