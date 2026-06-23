# 🔍 DupCleaner

A high-performance **duplicate file finder** built entirely in Go. Find duplicates across **images, videos, audio, documents, and archives** using perceptual image hashing (dHash, aHash, pHash), exact/partial MD5 matching, or smart multi-algorithm detection — all from a sleek single-page web UI.

![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Platform](https://img.shields.io/badge/Platform-Windows%20|%20macOS%20|%20Linux-blue)

## 📂 Supported File Types

| Category | Extensions |
|----------|-----------|
| **Images** | JPG, PNG, GIF, BMP, TIFF, WebP, HEIC/HEIF, AVIF, CR2, CR3, NEF, ARW, DNG, ORF, RW2, RAF, PEF, SRW, X3F, RAW, JXL, ICO, PSD |
| **Videos** | MP4, MKV, AVI, MOV, WMV, FLV, WebM, M4V, MPG, MPEG, 3GP, VOB, TS, MTS, M2TS, DIVX, RMVB, ASF, OGV |
| **Audio** | MP3, M4A, WAV, FLAC, OGG, AAC, WMA, OPUS, AIFF, AIF, ALAC, APE, DSD, DSF, MID, MIDI, AMR, AU, CAF |
| **Documents** | PDF, DOC, DOCX, RTF, ODT, TXT, MD, XLS, XLSX, CSV, ODS, TSV, PPT, PPTX, ODP, KEY, EPUB, MOBI, AZW, DJVU, HTML, XML, JSON |
| **Archives** | ZIP, RAR, 7Z, TAR, GZ, BZ2, XZ, TGZ, TBZ2, LZMA, CAB, ISO, DMG |

## ✨ Features

### Core
- **Multi-category scanning** — images, videos, audio, documents, archives, or all file types
- **5 detection modes** — Exact (MD5), Quick (MD5+dHash), Standard (MD5+pHash), Robust (MD5+dHash+aHash), Smart (all algorithms, 2-of-3 voting)
- **VP-tree powered** — O(N log N) perceptual matching instead of brute-force O(N²)
- **Size pre-grouping** — files with unique sizes skip expensive hash computation entirely
- **Partial hashing** — files >1 MB use 3-chunk partial MD5 (first + middle + last 64 KB) for blazing speed on large videos/archives

### Performance & Memory
- **Two-stage pipeline** — I/O workers saturate disk, CPU workers decode + hash in parallel
- **Separate CPU & RAM controls** — **Parallel jobs** sets the worker count (CPU load); a separate **Memory budget** slider sets how much RAM the scan may use. They're decoupled: the RAM budget caps how many images decode at once (`budget ÷ ~96 MB`) *independently* of the worker count, so you can keep CPU high while bounding memory — or, with lots of RAM, let more decode in parallel
- **Responsive by default** — recommended worker count leaves ~25% CPU headroom so the machine stays usable during a scan; "Full CPU" and oversubscribed presets are one click away
- **Bounded subprocess pool** — video/audio (`ffmpeg`/`fpcalc`) run in a small pool at below-normal priority, never pegging every core
- **Aggressive memory management** — `SetMemoryLimit` during scan, post-scan GC with OS page return (runs even on cancel)
- **Thumbnail disk cache** — 240px JPEG thumbnails cached in `thumbs/` with SHA1-based paths, survives restarts
- **Hash cache** — all computed hashes (image hashes, video frame + packet signatures, audio fingerprints) persisted to `cache.json`, making re-scans near-instant
- **Path-independent cache** — a moved or renamed file reuses its cached fingerprint instead of recomputing; a `(size, mtime)` lookup finds the candidate and a content check confirms identical bytes before reuse, so files that merely share a size + timestamp are never mistaken for one another

### Image Support
- **Native decoders** — JPEG, PNG, GIF, BMP, TIFF, WebP, HEIC/HEIF (via pure-Go WASM decoder)
- **External fallback** — AVIF, RAW (CR2/CR3/NEF/ARW/DNG), PSD, JXL, ICO via `sips`/`magick`/`ffmpeg`
- **Perceptual hashing** — find visually similar images even with different compression/resolution

### Non-Image Files (Videos, Audio, Docs, Archives)
- **Content-based matching** — exact MD5 or partial hash (3-chunk sampling for files >1 MB)
- **Category-aware UI** — file-type icons, video/audio preview in browser, download for docs/archives
- **Same speed** — partial hashing makes scanning terabytes of video as fast as small image folders

### Visual Video Matching
- **Frame-based fingerprinting** — the Videos category offers a "Visual (frame match)" mode
  that samples frames (via ffmpeg) and perceptually hashes them, so it finds the **same video
  across different resolutions** (1080p/720p) and **formats** (mp4/mkv) — not just byte-identical copies
- **Strictness control** — Strict / Balanced / Aggressive bar trades recall for fewer false positives
- **Duration pre-filter** — videos with no duration-twin skip frame extraction (fast)
- **Configurable tools** — `ffmpeg`/`ffprobe` are auto-detected from PATH. If missing, either click
  **⬇ Download ffmpeg** in the UI to fetch the official build for your OS automatically, or point to the
  binaries yourself (native file picker). Paths are saved to `tools.json`; falls back to exact matching if absent

### Acoustic Audio Matching
- **Chromaprint fingerprinting** — the Audio category offers an "Acoustic (fingerprint)" mode that
  computes a Chromaprint fingerprint per track (via `fpcalc`), so it finds the **same song across
  different bitrates** (MP3 320 vs 128) and **encodings** (MP3/FLAC/M4A) — not just byte-identical files
- **Strictness control** — Strict / Balanced / Aggressive trades recall for fewer false positives
- **Duration pre-filter** — tracks with no duration-twin skip fingerprinting (fast)
- **Configurable tool** — `fpcalc` is auto-detected from PATH. If missing, click **⬇ Download fpcalc**
  in the UI for a one-click install of the official Chromaprint build, or point to it yourself (saved to
  `tools.json`). Falls back to exact matching if absent
- **One-click tool install** — ffmpeg/ffprobe and fpcalc can be auto-downloaded from their official
  sources, verified, and wired into the app from the Videos/Audio tool panels (saved under `tools/`)

### Auto-Cleanup Rules
- **Saved selection strategies** — save a Smart Select strategy (keep highest-res / largest / oldest /
  newest / prefer-folder) as a named rule, persisted to `rules.json`, and re-apply it to any future
  scan with one click
- **Safe by design** — applying a rule only *selects* duplicates; deletion still requires your explicit
  confirmation (no automatic deletion)

### Network Shares / NAS
- **Scan UNC and mounted paths** — type a Windows UNC path (`\\server\share\folder`) or a mounted NAS
  path (`/mnt/nas`) directly into the directory box; the scanner walks them like local folders.
  (The native folder picker may not browse network locations — typing the path is the reliable way.)
- **Resilient walks** — unreadable files/subdirs on a flaky share are logged and skipped, not fatal

### UI & UX
- **Single-page web UI** — one HTML file, no build step, no npm; **embedded in the binary** (ship just the `.exe`)
- **Light & dark themes** — toggle in the sidebar, persisted across sessions; even server-drawn file-type icons adapt to the theme
- **Reclaimable-space hero** — on scan complete, a headline "X GB reclaimable" with group/file counts, plus a clear **Smart Cleanup** (auto-select the safe duplicates) vs **Review** fork
- **Honest KEEP / DELETE badges** — every copy shows a live green **KEEP** or red **DELETE** badge reflecting the *current* selection, so it's always obvious what survives — whichever strategy you use
- **Folder tools** — a folder-breakdown panel (which folders hold duplicates, with reclaimable size), per-folder filtering, and folder-based selection
- **Live progress** — real-time file count, processing rate, heap memory usage
- **Smart selection** — auto-select duplicates to delete by: highest resolution, largest file, oldest, newest, or preferred directory
- **Side-by-side preview** — in-browser compare of two copies, plus **Open original** (launch in the OS default app) and **Reveal in folder**
- **Paginated results** — large result sets render one page of groups at a time, keeping the UI responsive and thumbnail loading bounded no matter how many duplicates were found
- **Batch operations** — select all duplicates / everything / clear *across all pages* (selection spans the whole result set, not just the visible page), delete to trash or permanently, export CSV report
- **Session persistence** — save/load scan results across server restarts
- **Native folder picker** — OS-native folder selection dialog (PowerShell/AppleScript/Zenity)

### Safety
- **Recycle Bin support** — deleted files go to Trash/Recycle Bin by default (Windows batch PowerShell, macOS Finder, Linux gio)
- **No-original-loss guard** — if a delete would remove *every* copy of a group (leaving nothing), you get an explicit warning before it proceeds
- **Selection is never automatic** — Smart Cleanup / rules only *select*; files are removed only on your confirmation
- **Cancel anytime** — Stop aborts promptly at every stage (folder enumeration, hashing, video/audio fingerprinting); partial cache is preserved
- **No external services** — everything runs locally, no data leaves your machine

## 🚀 Quick Start

### Prerequisites
- Go 1.25+ installed ([download](https://go.dev/dl/))

### Run

```bash
# Clone the repo
git clone https://github.com/rahul1996pp/DupCleaner.git
cd DupCleaner

# Run directly
go run .

# Or build and run
go build -o dupcleaner.exe .     # Windows: build.bat
./dupcleaner.exe
```

The HTML UI is **embedded in the binary**, so the build is a single self-contained executable — distribute just `dupcleaner.exe`, run it anywhere, no other files required. (Dropping an `index.html` next to it overrides the embedded copy for live dev edits.)

Open [http://localhost:7891](http://localhost:7891) in your browser.

### Custom Port

```bash
./dupcleaner.exe 8080
```

## 🐳 Docker

Run DupCleaner in a container with `ffmpeg`/`ffprobe` (video matching) and `chromaprint`/`fpcalc` (audio matching) preinstalled.

### Build

```bash
docker build -t dupcleaner:latest .
```

### Quick run

```bash
docker run --rm -p 7891:7891 -v dupcleaner-data:/data dupcleaner:latest
```

Then open http://localhost:7891.

- `-p 7891:7891` maps the port (the container binds `0.0.0.0` via `DUPCLEANER_HOST=0.0.0.0`, set in the image).
- `-v dupcleaner-data:/data` persists app state (`cache.json`, `session.json`, `tools.json`, `rules.json`, `dupcleaner.log`, `thumbs/`) in a named volume.
- Change the port by passing it as the first argument, e.g. `docker run ... dupcleaner:latest 8080` (also update `-p`).

### Run with Docker Compose

Edit the media bind mount in `docker-compose.yml` (replace `/path/to/your/media` with a real host path), then:

```bash
docker compose up -d        # start in the background
docker compose logs -f      # follow logs
docker compose down         # stop (keeps the data volume)
```

### Scanning a host directory

Mount the folder you want to scan into the container, then point the app at the in-container path. The example compose file mounts it read-only at `/media/photos`:

```yaml
volumes:
  - /path/to/your/media:/media/photos:ro
```

With `docker run`:

```bash
docker run --rm -p 7891:7891 \
  -v dupcleaner-data:/data \
  -v /path/to/your/media:/media/photos:ro \
  dupcleaner:latest
```

Scan `/media/photos` from inside the app. The `:ro` flag mounts media read-only so scans cannot modify your originals; add more `-v` lines for additional folders.

### Persistent data

App state lives in the `/data` volume. To reset everything (cache, session, thumbnails, logs):

```bash
docker compose down -v            # with compose
docker volume rm dupcleaner-data  # with plain docker run
```

### Caveats (headless)

- **Native folder/file pickers** and **"open in folder"** rely on the host desktop and do **not** work in a container — type/paste paths into the UI instead, using the in-container mount paths (e.g. `/media/photos`).
- **"Move to trash"** uses the OS recycle bin and is unavailable headless; deletions in a container are permanent, so prefer read-only media mounts (`:ro`).
- **Video and audio matching DO work** — `ffmpeg`, `ffprobe`, and `fpcalc` (chromaprint) are installed in the image, so no extra setup is needed.

## 📁 Project Structure

```
.
├── main.go              # Thin entry point — calls internal/dup.Main()
├── internal/
│   └── dup/             # All application logic + unit tests (kept beside the code)
│       ├── main.go      # HTTP server, scan engine, image hashing, VP-tree, core logic
│       ├── video.go     # Video frame-based perceptual fingerprinting (ffmpeg/ffprobe)
│       ├── audio.go     # Audio Chromaprint fingerprinting (fpcalc)
│       ├── cleanup.go   # Saved auto-cleanup rules (selection strategies)
│       ├── tools.go     # External tool config + native file/folder pickers
│       ├── download.go  # One-click auto-download of ffmpeg/ffprobe/fpcalc
│       ├── proc_windows.go / proc_other.go   # Subprocess priority (build-tagged)
│       ├── index.html   # Full SPA frontend (HTML + CSS + JS, embedded into the binary)
│       └── *_test.go    # Unit tests, beside the code they exercise
├── build.bat            # One-command Windows build
├── bench.bat            # Per-machine CPU/RAM benchmark sweep
├── go.mod / go.sum      # Go module definition
├── Dockerfile           # Multi-stage container image (ffmpeg + chromaprint)
├── docker-compose.yml
├── tools/               # Auto-downloaded tool binaries (auto-created)
├── cache.json           # Persistent hash cache (auto-created)
├── session.json         # Saved scan session (auto-created)
├── tools.json           # Configured tool paths (auto-created)
├── rules.json           # Saved auto-cleanup rules (auto-created)
├── dupcleaner.log       # Runtime log file
└── thumbs/              # Disk thumbnail cache (256 subdirs, auto-created)
```

## 🔌 API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Serve web UI |
| POST | `/api/scan` | Start a new scan |
| POST | `/api/scan/cancel` | Cancel running scan |
| GET | `/api/progress` | Poll scan progress (total, processed, rate, heap) |
| GET | `/api/results` | Get duplicate groups |
| GET | `/api/preview?path=...` | Full-size image/video/audio preview |
| GET | `/api/thumbnail?path=...` | 240px cached thumbnail |
| POST | `/api/delete` | Delete files (trash or permanent) |
| GET | `/api/open?path=...` | Reveal file location in OS file manager |
| GET | `/api/open-file?path=...` | Open the original file in its default app (scan-results allowlist + CSRF guard) |
| GET | `/api/export` | Export results as CSV |
| POST | `/api/smart-select` | Auto-select duplicates by strategy |
| GET | `/api/health` | Server health check |
| GET | `/api/system` | System info (CPU, RAM, recommended workers) |
| GET | `/api/cache/stats` | Cache hit/miss statistics |
| POST | `/api/cache/clear` | Clear hash + thumbnail cache |
| POST | `/api/session/save` | Save current results to disk |
| GET | `/api/session/load` | Load saved session |
| GET | `/api/session/exists` | Check if a saved session exists |
| POST | `/api/session/clear` | Delete saved session |
| POST | `/api/folder/pick` | Open native OS folder picker |
| POST | `/api/file/pick` | Open native OS file picker (e.g. select a binary) |
| GET/POST | `/api/video/tools` | Get status of / set paths to ffmpeg & ffprobe |
| GET/POST | `/api/audio/tools` | Get status of / set path to fpcalc (Chromaprint) |
| POST | `/api/tools/download` | One-click download + install of ffmpeg/ffprobe or fpcalc |
| GET | `/api/rules` | List saved auto-cleanup rules |
| POST | `/api/rules/save` | Create/update a saved rule |
| POST | `/api/rules/delete` | Delete a saved rule |
| POST | `/api/rules/apply` | Apply a saved rule to current results (returns selection) |
| POST | `/api/log` | Client-side error logging |
| GET | `/api/logs` | View recent server logs |

## 🧠 How Detection Works

### Exact Matching (MD5)
Files are grouped by size first. Only files sharing a size get hashed. Files ≤1 MB use full MD5; larger files use a 3-chunk partial hash (first/middle/last 64 KB + file size) for speed.

### Perceptual Hashing
Images are decoded to NRGBA, then:
- **dHash** — 9×8 difference hash, compares adjacent pixel luminance
- **aHash** — 8×8 average hash, compares each pixel to the mean
- **pHash** — 32×32 DCT-based hash, robust to compression and resizing

### VP-Tree Search
Instead of comparing every pair (O(N²)), a Vantage Point Tree partitions the hash space. Each image query is O(log N), making the total grouping O(N log N).

### Smart Mode (2-of-3 Voting)
When all three perceptual algorithms are active, images are only grouped if at least 2 of the 3 hash types agree within the threshold. This dramatically reduces false positives.

## 📋 Configuration

No config files needed. Everything is controlled from the web UI.

| Setting | Default | Notes |
|---------|---------|-------|
| Port | 7891 | Override via CLI argument |
| Category | Images | Images, Videos, Audio, Documents, Archives, or All |
| Detection mode | Quick | Exact + dHash |
| Threshold | 5 | Hamming distance (0–20, images only) |
| Parallel jobs (CPU) | Recommended (~75% of cores) | Worker count; presets: Economy / Recommended / Full CPU / Max |
| Memory budget (RAM) | ~70% of installed | Caps total memory **and** concurrent decodes, independent of CPU |
| Cache | Enabled | Persists to `cache.json` |
| Min file size | 0 KB | Filter small files |
| Skip hidden | true | Ignore dotfiles/folders |
| Exclude dirs | — | Skip specific directories |

## 🔒 Security Notes

- All processing is local — no data leaves your machine
- No external API calls or telemetry
- Files are never modified — only deleted (to trash) when you explicitly confirm
- The server binds to `127.0.0.1` only — not accessible from other machines
- **"Open original" is hardened** — only files found in the current scan can be launched, executable types are refused, and cross-site requests are rejected (no `cmd.exe` shell-out)
- Minimal dependencies — only `golang.org/x/image` and a pure-Go HEIC decoder

## 🧪 Testing

```bash
go build ./...
go vet ./...
go test ./...
```

**Benchmark your machine** — sweep worker counts across images/videos/audio and get a recommended CPU/RAM setting for each file type:

```bash
bench.bat                 # synthetic media (needs ffmpeg on PATH)
bench.bat "D:\My Photos"  # benchmark against your real files
```

## 🛣️ Roadmap

- [ ] GPU-accelerated image decoding
- [x] Video frame-based duplicate detection
- [x] Audio fingerprinting (Chromaprint)
- [x] Network share / NAS scanning
- [x] Docker container support
- [x] Configurable auto-cleanup rules

## 📄 License

Apache License 2.0 — See [LICENSE](LICENSE) for details.

## 👤 Author

**Rahul P** — [@rahul1996pp](https://github.com/rahul1996pp)

If you find this useful, give it a ⭐ on [GitHub](https://github.com/rahul1996pp/DupCleaner)!
