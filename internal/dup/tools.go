package dup

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
)

const toolsConfigFile = "tools.json"

// ToolsConfig holds user-specified paths to external tools. An empty path means
// "auto-detect from PATH".
type ToolsConfig struct {
	FFmpeg  string `json:"ffmpeg"`
	FFprobe string `json:"ffprobe"`
	FPcalc  string `json:"fpcalc"` // Chromaprint fpcalc, for audio acoustic matching
}

var (
	toolsCfg   ToolsConfig
	toolsCfgMu sync.RWMutex
	// toolsSaveMu serializes the marshal+write in saveToolsConfig so concurrent
	// callers (set-path / auto-detect / auto-download) can't interleave the
	// os.WriteFile and corrupt tools.json.
	toolsSaveMu sync.Mutex
)

// loadToolsConfig reads tools.json if present. Missing file is not an error.
func loadToolsConfig() {
	data, err := os.ReadFile(toolsConfigFile)
	if err != nil {
		return
	}
	var c ToolsConfig
	if err := json.Unmarshal(data, &c); err != nil {
		logf("WARN", "tools.json parse failed (ignoring): %v", err)
		return
	}
	toolsCfgMu.Lock()
	toolsCfg = c
	toolsCfgMu.Unlock()
	logf("INFO", "Loaded tool paths: ffmpeg=%q ffprobe=%q fpcalc=%q", c.FFmpeg, c.FFprobe, c.FPcalc)
}

// saveToolsConfig persists the current config to tools.json.
func saveToolsConfig() error {
	toolsSaveMu.Lock()
	defer toolsSaveMu.Unlock()
	toolsCfgMu.RLock()
	data, err := json.MarshalIndent(toolsCfg, "", "  ")
	toolsCfgMu.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(toolsConfigFile, data, 0644)
}

// ffmpegBin returns the configured ffmpeg path, or "ffmpeg" for PATH lookup.
func ffmpegBin() string {
	toolsCfgMu.RLock()
	defer toolsCfgMu.RUnlock()
	if toolsCfg.FFmpeg != "" {
		return toolsCfg.FFmpeg
	}
	return "ffmpeg"
}

// ffprobeBin returns the configured ffprobe path, or "ffprobe" for PATH lookup.
func ffprobeBin() string {
	toolsCfgMu.RLock()
	defer toolsCfgMu.RUnlock()
	if toolsCfg.FFprobe != "" {
		return toolsCfg.FFprobe
	}
	return "ffprobe"
}

// resolveTool returns the path a bin name/path resolves to, and whether it is
// usable. A path containing a separator is checked on disk; a bare name is
// looked up on PATH.
func resolveTool(bin string) (string, bool) {
	if bin == "" {
		return "", false
	}
	if strings.ContainsAny(bin, `/\`) {
		if fi, err := os.Stat(bin); err == nil && !fi.IsDir() {
			return bin, true
		}
		return bin, false
	}
	if p, err := exec.LookPath(bin); err == nil {
		return p, true
	}
	return bin, false
}

// validateTool runs "<bin> -version" to confirm the binary actually works.
func validateTool(bin string) bool {
	if bin == "" {
		return false
	}
	return exec.Command(bin, "-version").Run() == nil
}

type toolsStatusResp struct {
	FFmpegPath      string `json:"ffmpeg_path"`  // configured (empty = auto)
	FFprobePath     string `json:"ffprobe_path"`
	FFmpegResolved  string `json:"ffmpeg_resolved"`
	FFprobeResolved string `json:"ffprobe_resolved"`
	FFmpegFound     bool   `json:"ffmpeg_found"`
	FFprobeFound    bool   `json:"ffprobe_found"`
}

func currentToolsStatus() toolsStatusResp {
	toolsCfgMu.RLock()
	fp, pp := toolsCfg.FFmpeg, toolsCfg.FFprobe
	toolsCfgMu.RUnlock()
	fRes, fOK := resolveTool(ffmpegBin())
	pRes, pOK := resolveTool(ffprobeBin())
	return toolsStatusResp{
		FFmpegPath: fp, FFprobePath: pp,
		FFmpegResolved: fRes, FFprobeResolved: pRes,
		FFmpegFound: fOK, FFprobeFound: pOK,
	}
}

// handleVideoTools: GET returns status; POST sets paths (empty string = auto).
func handleVideoTools(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost {
		var req struct {
			FFmpeg  *string `json:"ffmpeg"`
			FFprobe *string `json:"ffprobe"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.FFmpeg != nil {
			p := strings.TrimSpace(*req.FFmpeg)
			if p != "" && !validateTool(p) {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Not a working ffmpeg binary"})
				return
			}
			toolsCfgMu.Lock()
			toolsCfg.FFmpeg = p
			toolsCfgMu.Unlock()
		}
		if req.FFprobe != nil {
			p := strings.TrimSpace(*req.FFprobe)
			if p != "" && !validateTool(p) {
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Not a working ffprobe binary"})
				return
			}
			toolsCfgMu.Lock()
			toolsCfg.FFprobe = p
			toolsCfgMu.Unlock()
		}
		if err := saveToolsConfig(); err != nil {
			logf("WARN", "tools.json save failed: %v", err)
		}
		st := currentToolsStatus()
		logf("INFO", "Tool paths updated: ffmpeg=%q(found=%v) ffprobe=%q(found=%v)",
			st.FFmpegResolved, st.FFmpegFound, st.FFprobeResolved, st.FFprobeFound)
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "status": st})
		return
	}
	json.NewEncoder(w).Encode(currentToolsStatus())
}

// handleFilePick opens a native file picker and returns the chosen path.
// POST-only so a cross-site GET (e.g. <img src>) can't pop a native dialog.
func handleFilePick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	path := pickFile()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"path": path})
}

// pickFile shows an OS file-open dialog and returns one absolute path, or "".
func pickFile() string {
	switch runtime.GOOS {
	case "windows":
		return pickFileWindows()
	case "darwin":
		return pickFileMacOS()
	default:
		return pickFileLinux()
	}
}

func pickFileWindows() string {
	script := `
[void][System.Reflection.Assembly]::LoadWithPartialName('System.Windows.Forms')
Add-Type -AssemblyName System.Windows.Forms
$form = New-Object System.Windows.Forms.Form
$form.TopMost = $true
$form.Opacity = 0
$form.ShowInTaskbar = $false
$form.StartPosition = 'CenterScreen'
$form.Show()
$form.Focus() | Out-Null
$d = New-Object System.Windows.Forms.OpenFileDialog
$d.Title = 'Select binary (ffmpeg / ffprobe)'
$d.Filter = 'Executables (*.exe)|*.exe|All files (*.*)|*.*'
$result = $d.ShowDialog($form)
$form.Close()
if ($result -eq [System.Windows.Forms.DialogResult]::OK) {
    Write-Output $d.FileName
}
`
	out, err := exec.Command("powershell", "-NoProfile", "-STA", "-Command", script).Output()
	if err != nil {
		logf("ERROR", "File picker (PowerShell) failed: %v", err)
		return ""
	}
	return strings.TrimSpace(string(out))
}

func pickFileMacOS() string {
	script := `set f to choose file with prompt "Select binary (ffmpeg / ffprobe)"
return POSIX path of f`
	out, err := exec.Command("osascript", "-e", script).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func pickFileLinux() string {
	if out, err := exec.Command("zenity", "--file-selection", "--title=Select binary (ffmpeg / ffprobe)").Output(); err == nil {
		return strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("kdialog", "--getopenfilename", os.Getenv("HOME")).Output(); err == nil {
		return strings.TrimSpace(string(out))
	}
	logf("WARN", "No file picker tool available (tried zenity, kdialog)")
	return ""
}
