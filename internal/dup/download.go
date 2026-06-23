package dup

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// toolsDir is where auto-downloaded binaries are installed, relative to CWD
// (like cache.json / tools.json). The integrator should add it to .gitignore.
const toolsDir = "tools"

// dlSource describes how to fetch and unpack one archive for a package/OS/arch.
type dlSource struct {
	url          string
	checksumURL  string   // "" if no checksum file is available
	checksumKind string   // "sha256", "md5", or ""
	archive      string   // "zip", "targz", or "tarxz"
	bins         []string // binary basenames to extract+install
	// extraURL handles the macOS ffmpeg case where ffmpeg and ffprobe ship as
	// two separate single-binary zips. When set, it is fetched as a second zip
	// and extraBins are pulled from it. "" / nil for the common single-archive
	// case.
	extraURL  string
	extraBins []string
}

// dlSources maps "package\x00goos\x00goarch" to its download spec. Only the
// hardcoded official HTTPS URLs below are ever fetched — no user-supplied URLs.
var dlSources = map[string]dlSource{
	// ── ffmpeg (installs both ffmpeg and ffprobe) ──────────────────────────
	dlKey("ffmpeg", "windows", "amd64"): {
		url:          "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip",
		checksumURL:  "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip.sha256",
		checksumKind: "sha256",
		archive:      "zip",
		bins:         []string{"ffmpeg.exe", "ffprobe.exe"},
	},
	dlKey("ffmpeg", "linux", "amd64"): {
		url:          "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz",
		checksumURL:  "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz.md5",
		checksumKind: "md5",
		archive:      "tarxz",
		bins:         []string{"ffmpeg", "ffprobe"},
	},
	dlKey("ffmpeg", "linux", "arm64"): {
		url:          "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz",
		checksumURL:  "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz.md5",
		checksumKind: "md5",
		archive:      "tarxz",
		bins:         []string{"ffmpeg", "ffprobe"},
	},
	// macOS: ffmpeg and ffprobe ship as separate zips with no stable checksum
	// file; rely on validateTool as the functional gate.
	dlKey("ffmpeg", "darwin", "amd64"): {
		url:       "https://evermeet.cx/ffmpeg/getrelease/ffmpeg/zip",
		archive:   "zip",
		bins:      []string{"ffmpeg"},
		extraURL:  "https://evermeet.cx/ffmpeg/getrelease/ffprobe/zip",
		extraBins: []string{"ffprobe"},
	},
	dlKey("ffmpeg", "darwin", "arm64"): {
		url:       "https://evermeet.cx/ffmpeg/getrelease/ffmpeg/zip",
		archive:   "zip",
		bins:      []string{"ffmpeg"},
		extraURL:  "https://evermeet.cx/ffmpeg/getrelease/ffprobe/zip",
		extraBins: []string{"ffprobe"},
	},

	// ── fpcalc (Chromaprint) ───────────────────────────────────────────────
	dlKey("fpcalc", "windows", "amd64"): {
		url:     "https://github.com/acoustid/chromaprint/releases/download/v1.5.1/chromaprint-fpcalc-1.5.1-windows-x86_64.zip",
		archive: "zip",
		bins:    []string{"fpcalc.exe"},
	},
	dlKey("fpcalc", "linux", "amd64"): {
		url:     "https://github.com/acoustid/chromaprint/releases/download/v1.5.1/chromaprint-fpcalc-1.5.1-linux-x86_64.tar.gz",
		archive: "targz",
		bins:    []string{"fpcalc"},
	},
	dlKey("fpcalc", "darwin", "amd64"): {
		url:     "https://github.com/acoustid/chromaprint/releases/download/v1.5.1/chromaprint-fpcalc-1.5.1-macos-x86_64.tar.gz",
		archive: "targz",
		bins:    []string{"fpcalc"},
	},
	dlKey("fpcalc", "darwin", "arm64"): {
		url:     "https://github.com/acoustid/chromaprint/releases/download/v1.5.1/chromaprint-fpcalc-1.5.1-macos-arm64.tar.gz",
		archive: "targz",
		bins:    []string{"fpcalc"},
	},
	// linux/arm64 fpcalc is not published by upstream → unsupported.
}

func dlKey(pkg, goos, goarch string) string {
	return pkg + "\x00" + goos + "\x00" + goarch
}

// officialLink returns the upstream download page for a package, shown to the
// user when their OS/arch combo is unsupported.
func officialLink(pkg string) string {
	if pkg == "fpcalc" {
		return "https://github.com/acoustid/chromaprint/releases"
	}
	return "https://ffmpeg.org/download.html"
}

// handleToolDownload downloads, verifies, extracts, validates and installs the
// official binaries for a tool package, then wires them into the tools config.
//
// Request (POST, JSON):
//
//	{"tool":"ffmpeg"}   // installs ffmpeg + ffprobe
//	{"tool":"fpcalc"}   // installs fpcalc
//
// Response (always HTTP 200, JSON):
//
//	{"ok":true,"message":"...","status":{...}}                 // success
//	{"ok":false,"error":"...","link":"https://..."}            // unsupported OS/arch (link set)
//	{"ok":false,"error":"checksum mismatch"}                   // verification failed
//	{"ok":false,"error":"downloaded ffprobe did not run"}      // validation failed
//
// On success the ffmpeg package sets toolsCfg.FFmpeg + toolsCfg.FFprobe and the
// "status" field is currentToolsStatus(); the fpcalc package sets toolsCfg.FPcalc
// and returns a small {"fpcalc_path","fpcalc_found"} status map.
func handleToolDownload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}

	var req struct {
		Tool string `json:"tool"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	pkg := strings.TrimSpace(req.Tool)
	if pkg != "ffmpeg" && pkg != "fpcalc" {
		writeDLResult(w, dlResult{Error: fmt.Sprintf("unknown tool %q (want \"ffmpeg\" or \"fpcalc\")", req.Tool)})
		return
	}

	src, ok := dlSources[dlKey(pkg, runtime.GOOS, runtime.GOARCH)]
	if !ok {
		writeDLResult(w, dlResult{
			Error: fmt.Sprintf("%s auto-download is not available for %s/%s — please install it manually", pkg, runtime.GOOS, runtime.GOARCH),
			Link:  officialLink(pkg),
		})
		return
	}

	if err := os.MkdirAll(toolsDir, 0755); err != nil {
		writeDLResult(w, dlResult{Error: "could not create tools directory: " + err.Error()})
		return
	}

	logf("INFO", "Tool download started: %s (%s/%s) from %s", pkg, runtime.GOOS, runtime.GOARCH, src.url)

	// Fetch + extract the primary archive (and the optional macOS extra zip).
	if err := fetchAndExtract(src.url, src.checksumURL, src.checksumKind, src.archive, src.bins); err != nil {
		writeDLResult(w, dlResult{Error: err.Error()})
		return
	}
	if src.extraURL != "" {
		if err := fetchAndExtract(src.extraURL, "", "", "zip", src.extraBins); err != nil {
			writeDLResult(w, dlResult{Error: err.Error()})
			return
		}
	}

	// Functional gate: every installed binary must actually run.
	allBins := append(append([]string{}, src.bins...), src.extraBins...)
	for _, b := range allBins {
		if !validateTool(filepath.Join(toolsDir, b)) {
			writeDLResult(w, dlResult{Error: fmt.Sprintf("downloaded %s did not run", strings.TrimSuffix(b, ".exe"))})
			return
		}
	}

	// Wire the verified paths into the live config and persist.
	if pkg == "ffmpeg" {
		ffmpegAbs, _ := filepath.Abs(filepath.Join(toolsDir, binFor(src.bins, "ffmpeg")))
		ffprobeAbs, _ := filepath.Abs(filepath.Join(toolsDir, binFor(allBins, "ffprobe")))
		toolsCfgMu.Lock()
		toolsCfg.FFmpeg = ffmpegAbs
		toolsCfg.FFprobe = ffprobeAbs
		toolsCfgMu.Unlock()
		if err := saveToolsConfig(); err != nil {
			logf("WARN", "tools.json save failed: %v", err)
		}
		logf("INFO", "ffmpeg installed: ffmpeg=%q ffprobe=%q", ffmpegAbs, ffprobeAbs)
		writeDLResult(w, dlResult{
			OK:      true,
			Message: "ffmpeg and ffprobe downloaded and installed",
			Status:  currentToolsStatus(),
		})
		return
	}

	// fpcalc
	fpcalcAbs, _ := filepath.Abs(filepath.Join(toolsDir, src.bins[0]))
	toolsCfgMu.Lock()
	toolsCfg.FPcalc = fpcalcAbs
	toolsCfgMu.Unlock()
	if err := saveToolsConfig(); err != nil {
		logf("WARN", "tools.json save failed: %v", err)
	}
	logf("INFO", "fpcalc installed: fpcalc=%q", fpcalcAbs)
	writeDLResult(w, dlResult{
		OK:      true,
		Message: "fpcalc downloaded and installed",
		Status: map[string]any{
			"fpcalc_path":  fpcalcAbs,
			"fpcalc_found": validateTool(fpcalcAbs),
		},
	})
}

// dlResult is the JSON response shape for handleToolDownload.
type dlResult struct {
	OK      bool        `json:"ok"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Link    string      `json:"link,omitempty"`
	Status  interface{} `json:"status,omitempty"`
}

func writeDLResult(w http.ResponseWriter, res dlResult) {
	json.NewEncoder(w).Encode(res)
}

// binFor returns the first basename in bins whose name (without .exe) matches
// want. Falls back to "<want>" + platform exe suffix if not found.
func binFor(bins []string, want string) string {
	for _, b := range bins {
		if strings.TrimSuffix(b, ".exe") == want {
			return b
		}
	}
	if runtime.GOOS == "windows" {
		return want + ".exe"
	}
	return want
}

// fetchAndExtract downloads url to a temp file (streaming), optionally verifies
// its checksum, extracts it into a temp dir, then copies each wanted bin into
// toolsDir. All temporary files are removed before returning.
func fetchAndExtract(url, checksumURL, checksumKind, archive string, bins []string) error {
	tmpFile, err := os.CreateTemp("", "dupcleaner-dl-*"+archiveExt(archive))
	if err != nil {
		return fmt.Errorf("temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if err := downloadToFile(url, tmpFile); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing download: %w", err)
	}

	if checksumKind != "" && checksumURL != "" {
		want, err := fetchChecksum(checksumURL)
		if err != nil {
			return err
		}
		got, err := fileDigest(tmpPath, checksumKind)
		if err != nil {
			return err
		}
		if !strings.EqualFold(want, got) {
			return fmt.Errorf("checksum mismatch")
		}
	}

	destDir, err := os.MkdirTemp("", "dupcleaner-extract-*")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	defer os.RemoveAll(destDir)

	switch archive {
	case "zip":
		err = extractZip(tmpPath, destDir)
	case "targz":
		err = extractTarGz(tmpPath, destDir)
	case "tarxz":
		err = extractTarXz(tmpPath, destDir)
	default:
		err = fmt.Errorf("unsupported archive type %q", archive)
	}
	if err != nil {
		return err
	}

	return installBins(destDir, bins)
}

func archiveExt(archive string) string {
	switch archive {
	case "zip":
		return ".zip"
	case "targz":
		return ".tar.gz"
	case "tarxz":
		return ".tar.xz"
	default:
		return ""
	}
}

// dlClient is used for all downloads. Generous timeout to allow ~100MB fetches.
var dlClient = &http.Client{Timeout: 15 * time.Minute}

// downloadToFile streams the response body of url into w. Some hosts require a
// User-Agent; redirects are followed by the default client transport.
func downloadToFile(url string, w io.Writer) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	req.Header.Set("User-Agent", "DupCleaner/1.0 (+https://github.com/acoustid/chromaprint)")
	resp, err := dlClient.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: HTTP %d", url, resp.StatusCode)
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		return fmt.Errorf("saving download: %w", err)
	}
	return nil
}

// fetchChecksum downloads a small checksum text file and returns the first
// whitespace-separated token (the hex digest).
func fetchChecksum(url string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("checksum request: %w", err)
	}
	req.Header.Set("User-Agent", "DupCleaner/1.0")
	resp, err := dlClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download checksum: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download checksum: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", fmt.Errorf("reading checksum: %w", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return "", fmt.Errorf("empty checksum file")
	}
	return fields[0], nil
}

// fileDigest computes the streaming sha256 or md5 hex digest of the file at path.
func fileDigest(path, kind string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var h hash.Hash
	switch kind {
	case "sha256":
		h = sha256.New()
	case "md5":
		h = md5.New()
	default:
		return "", fmt.Errorf("unsupported checksum kind %q", kind)
	}
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// installBins walks the extracted tree and copies each wanted binary (matched by
// basename) into toolsDir. On unix the copies are chmod 0755.
func installBins(srcDir string, bins []string) error {
	want := make(map[string]bool, len(bins))
	for _, b := range bins {
		want[b] = true
	}
	found := make(map[string]string, len(bins))

	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		base := filepath.Base(path)
		if want[base] {
			found[base] = path
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scanning archive: %w", err)
	}

	for _, b := range bins {
		srcPath, ok := found[b]
		if !ok {
			return fmt.Errorf("%s not found in downloaded archive", strings.TrimSuffix(b, ".exe"))
		}
		if err := copyExecutable(srcPath, filepath.Join(toolsDir, b)); err != nil {
			return fmt.Errorf("installing %s: %w", b, err)
		}
	}
	return nil
}

// copyExecutable copies src to dst, replacing dst if it exists. On unix the result is
// chmod 0755 so the binary is executable.
func copyExecutable(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(dst, 0755); err != nil {
			return err
		}
	}
	return nil
}

// safeJoin joins destDir and an archive entry name, rejecting paths that would
// escape destDir (zip-slip / tar-slip protection).
func safeJoin(destDir, name string) (string, error) {
	target := filepath.Join(destDir, name)
	cleanDest := filepath.Clean(destDir) + string(os.PathSeparator)
	if !strings.HasPrefix(filepath.Clean(target)+string(os.PathSeparator), cleanDest) {
		return "", fmt.Errorf("unsafe path in archive: %q", name)
	}
	return target, nil
}

// extractZip unpacks a zip archive into destDir, guarding against zip-slip.
func extractZip(zipPath, destDir string) error {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("opening zip: %w", err)
	}
	defer zr.Close()

	for _, f := range zr.File {
		target, err := safeJoin(destDir, f.Name)
		if err != nil {
			return err
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			rc.Close()
			return err
		}
		_, err = io.Copy(out, rc)
		rc.Close()
		out.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// extractTarGz unpacks a gzip-compressed tar archive into destDir, guarding
// against tar-slip.
func extractTarGz(tarPath, destDir string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("opening tar.gz: %w", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()
	return extractTarStream(tar.NewReader(gz), destDir)
}

// extractTarXz extracts a .tar.xz archive. Go's stdlib has no xz decoder, so we
// shell out to the system `tar` (used only on linux, where tar is present).
func extractTarXz(tarPath, destDir string) error {
	cmd := exec.Command("tar", "-xJf", tarPath, "-C", destDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("extracting tar.xz via system tar: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// extractTarStream reads tar entries from tr and writes regular files into
// destDir, guarding against tar-slip.
func extractTarStream(tr *tar.Reader, destDir string) error {
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading tar: %w", err)
		}
		target, err := safeJoin(destDir, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
			if err != nil {
				return err
			}
			_, err = io.Copy(out, tr)
			out.Close()
			if err != nil {
				return err
			}
		}
	}
}
