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
- **Adaptive worker count** — auto-scales to CPU cores with RAM safety cap
- **Aggressive memory management** — `SetMemoryLimit` during scan, post-scan GC with OS page return
- **Thumbnail disk cache** — 240px JPEG thumbnails cached in `thumbs/` with SHA1-based paths, survives restarts
- **Hash cache** — all computed hashes persisted to `cache.json`, making re-scans near-instant

### Image Support
- **Native decoders** — JPEG, PNG, GIF, BMP, TIFF, WebP, HEIC/HEIF (via pure-Go WASM decoder)
- **External fallback** — AVIF, RAW (CR2/CR3/NEF/ARW/DNG), PSD, JXL, ICO via `sips`/`magick`/`ffmpeg`
- **Perceptual hashing** — find visually similar images even with different compression/resolution

### Non-Image Files (Videos, Audio, Docs, Archives)
- **Content-based matching** — exact MD5 or partial hash (3-chunk sampling for files >1 MB)
- **Category-aware UI** — file-type icons, video/audio preview in browser, download for docs/archives
- **Same speed** — partial hashing makes scanning terabytes of video as fast as small image folders

### UI & UX
- **Single-page web UI** — one HTML file, no build step, no npm
- **Dark theme** — modern dark interface with responsive layout
- **Live progress** — real-time file count, processing rate, heap memory usage
- **Smart selection** — auto-select duplicates to delete by: highest resolution, largest file, oldest, newest, or preferred directory
- **Batch operations** — select all, delete to trash or permanently, export CSV report
- **Session persistence** — save/load scan results across server restarts
- **Native folder picker** — OS-native folder selection dialog (PowerShell/AppleScript/Zenity)

### Safety
- **Recycle Bin support** — deleted files go to Trash/Recycle Bin by default (Windows batch PowerShell, macOS Finder, Linux gio)
- **Cancel anytime** — scan can be cancelled mid-flight, partial cache is preserved
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
go build -o dupcleaner.exe .
./dupcleaner.exe
```

Open [http://localhost:7891](http://localhost:7891) in your browser.

### Custom Port

```bash
./dupcleaner.exe 8080
```

## 📁 Project Structure

```
.
├── main.go          # HTTP server, scan engine, image hashing, VP-tree, all logic
├── index.html       # Full SPA frontend (HTML + CSS + JS, embedded at build)
├── go.mod           # Go module definition
├── cache.json       # Persistent hash cache (auto-created)
├── session.json     # Saved scan session (auto-created)
├── dupcleaner.log   # Runtime log file
└── thumbs/          # Disk thumbnail cache (256 subdirs, auto-created)
    ├── 00/
    ├── 01/
    └── .../ff/
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
| GET | `/api/open?path=...` | Open file location in OS file manager |
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
| GET | `/api/folder/pick` | Open native OS folder picker |
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
| Workers | Auto | All CPU cores, capped by RAM |
| Cache | Enabled | Persists to `cache.json` |
| Min file size | 0 KB | Filter small files |
| Skip hidden | true | Ignore dotfiles/folders |
| Exclude dirs | — | Skip specific directories |

## 🔒 Security Notes

- All processing is local — no data leaves your machine
- No external API calls or telemetry
- Files are never modified — only deleted (to trash) when you explicitly confirm
- The server binds to `127.0.0.1` only — not accessible from other machines
- Minimal dependencies — only `golang.org/x/image` and a pure-Go HEIC decoder

## 🧪 Testing

```bash
go build ./...
go vet ./...
```

## 🛣️ Roadmap

- [ ] GPU-accelerated image decoding
- [ ] Video frame-based duplicate detection
- [ ] Audio fingerprinting (Chromaprint)
- [ ] Network share / NAS scanning
- [ ] Docker container support
- [ ] Configurable auto-cleanup rules

## 📄 License

MIT — See [LICENSE](LICENSE) for details.

## 👤 Author

**Rahul P** — [@rahul1996pp](https://github.com/rahul1996pp)

If you find this useful, give it a ⭐ on [GitHub](https://github.com/rahul1996pp/DupCleaner)!
