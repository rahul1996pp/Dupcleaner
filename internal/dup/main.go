package dup

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"io/fs"
	"log"
	"math"
	"math/bits"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/gen2brain/heic" // Native HEIC (WASM, no CGO)
	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

//go:embed index.html
var embeddedHTML []byte

// ─────────────────────────────────────────────────────────────────────
// Logger — writes to BOTH console and dupcleaner.log
// ─────────────────────────────────────────────────────────────────────

var (
	logFile     *os.File
	logMu       sync.Mutex
	recentLogs  = make([]string, 0, 1000)
	recentLogMu sync.Mutex

	// minLogRank gates logf: messages ranked below it are dropped. Defaults to
	// DEBUG (0) so the full picture shows for easy diagnosis; override with
	// DUPCLEANER_LOG_LEVEL=info|warn|error to quieten.
	minLogRank int32
)

// logRank orders levels for filtering. Unknown levels (rank absent) always print.
var logRank = map[string]int32{
	"DEBUG": 0, "INFO": 1, "CLIENT-INFO": 1, "WARN": 2, "ERROR": 3,
}

func initLogger() {
	var err error
	logFile, err = os.OpenFile("dupcleaner.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open log file: %v (continuing with stderr only)\n", err)
		log.SetOutput(os.Stderr)
		return
	}
	log.SetOutput(io.MultiWriter(os.Stderr, &logTeeWriter{}))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Default to DEBUG (most verbose) for easy understanding of what a scan does.
	lvl := strings.ToLower(os.Getenv("DUPCLEANER_LOG_LEVEL"))
	switch lvl {
	case "info":
		atomic.StoreInt32(&minLogRank, 1)
	case "warn", "warning":
		atomic.StoreInt32(&minLogRank, 2)
	case "error":
		atomic.StoreInt32(&minLogRank, 3)
	default:
		atomic.StoreInt32(&minLogRank, 0) // debug
		lvl = "debug"
	}
	logf("INFO", "Log level: %s (set DUPCLEANER_LOG_LEVEL=info to quieten)", strings.ToUpper(lvl))
}

type logTeeWriter struct{}

func (lt *logTeeWriter) Write(p []byte) (int, error) {
	logMu.Lock()
	defer logMu.Unlock()
	if logFile != nil {
		logFile.Write(p)
		logFile.Sync()
	}
	// keep ring buffer of last 1000 lines for /api/logs
	recentLogMu.Lock()
	line := strings.TrimRight(string(p), "\n")
	if line != "" {
		recentLogs = append(recentLogs, line)
		if len(recentLogs) > 1000 {
			// Copy to a new slice to release the old backing array.
			// Without this, the underlying array grows unbounded because
			// reslicing only moves the pointer — old entries stay in memory.
			trimmed := make([]string, 1000)
			copy(trimmed, recentLogs[len(recentLogs)-1000:])
			recentLogs = trimmed
		}
	}
	recentLogMu.Unlock()
	return len(p), nil
}

func logf(level, format string, args ...interface{}) {
	if r, ok := logRank[level]; ok && r < atomic.LoadInt32(&minLogRank) {
		return
	}
	log.Printf("["+level+"] "+format, args...)
}

// ─────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────

type ImageInfo struct {
	Path      string    `json:"path"`
	Name      string    `json:"name"`
	Dir       string    `json:"dir"`
	Size      int64     `json:"size"`
	Width     int       `json:"width"`
	Height    int       `json:"height"`
	Megapixel float64   `json:"megapixel"`
	Format    string    `json:"format"`
	ModTime   time.Time `json:"mod_time"`
	MD5       string    `json:"md5,omitempty"`
	DHash     uint64    `json:"dhash"`
	AHash     uint64    `json:"ahash"`
	PHash     uint64    `json:"phash"`
	Decoded   bool      `json:"decoded"`
	Duration  float64   `json:"duration,omitempty"` // video/audio length in seconds
	VHashes   []uint64  `json:"-"`                  // per-frame pHashes; not sent to client/session
	AHashes   []uint32  `json:"-"`                  // audio Chromaprint frames; not sent to client/session
	PSig      []float32 `json:"-"`                  // compressed-domain packet signature; not sent to client/session
	Quick     string    `json:"-"`                  // content identity for the rename-resilient cache
}

type DuplicateGroup struct {
	ID         int          `json:"id"`
	Images     []*ImageInfo `json:"images"`
	Exact      bool         `json:"exact"`
	Algorithm  string       `json:"algorithm"`
	Similarity float64      `json:"similarity"`
	TotalSize  int64        `json:"total_size"`
	WastedSize int64        `json:"wasted_size"`
}

type ScanRequest struct {
	Dirs        []string `json:"dirs"`
	Category    string   `json:"category"` // images, videos, audio, documents, archives, all
	Mode        string   `json:"mode"`     // "exact", "quick", "standard", "robust", "smart"
	Threshold   int      `json:"threshold"`
	SkipHidden  bool     `json:"skip_hidden"`
	MinSizeKB   int64    `json:"min_size_kb"`
	ExtsFilter  []string `json:"exts_filter"`
	UseCache    bool     `json:"use_cache"`
	ExcludeDirs []string `json:"exclude_dirs"`
	Threads     int      `json:"threads"`        // CPU: parallel workers. 0 = auto
	MemBudgetMB int64    `json:"mem_budget_mb"`  // RAM: max memory for the scan. 0 = auto (70% RAM)
}

type DeleteRequest struct {
	Paths   []string `json:"paths"`
	ToTrash bool     `json:"to_trash"`
}

type SmartSelectRequest struct {
	Strategy  string `json:"strategy"`
	PreferDir string `json:"prefer_dir"`
}

type LogRequest struct {
	Level string `json:"level"`
	Msg   string `json:"msg"`
	File  string `json:"file,omitempty"`
	Line  int    `json:"line,omitempty"`
	Stack string `json:"stack,omitempty"`
}

type ScanProgress struct {
	Total     int64   `json:"total"`
	Processed int64   `json:"processed"`
	Status    string  `json:"status"`
	Done      bool    `json:"done"`
	Groups    int     `json:"groups"`
	WastedMB  float64 `json:"wasted_mb"`
	Rate      float64 `json:"rate"`
	HeapMB    float64 `json:"heap_mb"` // current Go heap in use
}

type ThumbProgress struct {
	Total    int64  `json:"total"`
	Done     int64  `json:"done"`
	Failed   int64  `json:"failed"`
	Skipped  int64  `json:"skipped"`
	Status   string `json:"status"`
	Running  bool   `json:"running"`
	Complete bool   `json:"complete"`
}

type AppState struct {
	mu            sync.RWMutex
	scanning      bool
	progress      ScanProgress
	groups        []*DuplicateGroup
	settings      ScanRequest // last scan settings (for session save)
	thumbing      bool          // a thumbnail-generation job is running
	thumbProgress ThumbProgress // live progress for thumbnail generation
	undo          *undoRecord   // most recent move-to-trash deletion, for one-click restore
}

// undoRecord captures what's needed to reverse the most recent move-to-trash
// deletion: the trashed file paths (to pull back out of the Recycle Bin) and a
// snapshot of the results exactly as they were BEFORE the delete (to restore the
// grid). Only set for to-trash deletes — permanent deletes are not recoverable.
type undoRecord struct {
	paths    []string
	groups   []*DuplicateGroup
	wastedMB float64
	groupN   int
}

// ─────────────────────────────────────────────────────────────────────
// Cache — persists hash computations across scans
// ─────────────────────────────────────────────────────────────────────

type CacheEntry struct {
	Size    int64  `json:"s"`
	ModUnix int64  `json:"m"`
	Width   int    `json:"w,omitempty"`
	Height  int    `json:"h,omitempty"`
	Format  string `json:"f,omitempty"`
	MD5     string `json:"md5,omitempty"`
	// Quick is a content identity (the same partial/full hash used for exact-dup
	// detection). It lets a moved/renamed file — same bytes, new path — reuse its
	// cached fingerprints: a (size,mtime) lookup finds a candidate, then Quick is
	// re-checked against the file on disk before the fingerprint is trusted, so
	// two different files that merely share a size+timestamp never get confused.
	Quick string `json:"q,omitempty"`
	DHash   uint64 `json:"dh,omitempty"`
	AHash   uint64 `json:"ah,omitempty"`
	PHash   uint64 `json:"ph,omitempty"`
	Decoded bool   `json:"d,omitempty"`
	// Track which hashes have been computed.
	// CRITICAL: a missing PHash field defaults to 0, which is a valid hash
	// value. Without this, pHash-mode scans would treat all cached entries
	// as having PHash=0 and group them as matches.
	HasDhash bool `json:"hd,omitempty"`
	HasAhash bool `json:"ha,omitempty"`
	HasPhash bool `json:"hp,omitempty"`
	// Video frame fingerprint. HasVHashes guards against treating an empty
	// slice as a computed result (same reasoning as HasPhash above).
	Duration   float64  `json:"dur,omitempty"`
	VHashes    []uint64 `json:"vh,omitempty"`
	HasVHashes bool     `json:"hvh,omitempty"`
	// Compressed-domain packet signature (no-decode tier). HasPSig guards an
	// empty slice the same way HasVHashes does. Additive field — no cache
	// version bump needed; old entries simply lack it and recompute (cheap).
	PSig    []float32 `json:"ps,omitempty"`
	HasPSig bool      `json:"hps,omitempty"`
	// Audio Chromaprint fingerprint. HasAHashes guards against treating an
	// empty slice as a computed result (same reasoning as HasVHashes above).
	AHashes    []uint32 `json:"ah2,omitempty"`
	HasAHashes bool     `json:"hah,omitempty"`
}

type Cache struct {
	mu      sync.RWMutex
	Entries map[string]*CacheEntry `json:"e"`
	// bySig indexes entries by "size:modUnix" so a file that was moved or
	// renamed (same bytes, same mtime, new path) reuses its cached fingerprints
	// instead of being re-hashed/re-fingerprinted. Rebuilt from Entries on Load;
	// never serialized (the key is derivable from each entry).
	bySig  map[string]*CacheEntry
	hits   int64
	misses int64

	// Append-only journal (write-ahead log). Every Set appends ONE line the
	// instant a fingerprint is computed, so an interrupted scan (window closed,
	// crash, OOM, sleep) keeps all work done so far instead of throwing it away.
	// The full snapshot (Entries) is rewritten only at safe points — end of scan
	// and shutdown — and the journal is replayed on top of the snapshot at the
	// next startup, then truncated once folded in. Appending is O(1): the map is
	// never re-serialized per entry. A nil journal makes every append a no-op, so
	// unit tests and pre-startup Sets never touch the disk.
	jmu     sync.Mutex
	journal *os.File
	jpath   string
	jerr    bool // set once a journal write fails, so the warning is logged once
}

func NewCache() *Cache {
	return &Cache{
		Entries: make(map[string]*CacheEntry),
		bySig:   make(map[string]*CacheEntry),
	}
}

// sigKey identifies a file by content-stable attributes that survive a move or
// rename: byte size and modification time. Two genuinely different files would
// have to share an exact size AND an exact nanosecond mtime to collide — and if
// they do, they are byte-identical in practice, so reusing the fingerprint is
// still correct.
func sigKey(size, modUnix int64) string {
	return strconv.FormatInt(size, 10) + ":" + strconv.FormatInt(modUnix, 10)
}

const cacheFile = "cache.json"
const journalFile = "cache.log" // append-only write-ahead log, compacted into cacheFile

// journalRec is one appended line: a path and the fingerprint just computed for
// it. The journal is a flat list of these — no header, no versions.
type journalRec struct {
	P string      `json:"p"`
	E *CacheEntry `json:"e"`
}

func (c *Cache) Load(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var loaded Cache
	if err := json.Unmarshal(data, &loaded); err != nil {
		return err
	}
	if loaded.Entries == nil {
		loaded.Entries = make(map[string]*CacheEntry)
	}
	c.Entries = loaded.Entries
	// Rebuild the (size,mtime) index — it is not serialized.
	c.bySig = make(map[string]*CacheEntry, len(loaded.Entries))
	for _, e := range loaded.Entries {
		c.bySig[sigKey(e.Size, e.ModUnix)] = e
	}
	return nil
}

func (c *Cache) Save(path string) error {
	c.mu.RLock()
	data, err := json.Marshal(c)
	c.mu.RUnlock()
	if err != nil {
		return err
	}
	// Atomic replace: write a sibling temp file, then rename over the target. A
	// crash or kill mid-write can never truncate the existing snapshot — the
	// rename either fully happens or not at all.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	// The snapshot now contains every journaled entry, so start the journal over;
	// this keeps it from growing without bound across scans. Safe because Save is
	// only called at scan boundaries when no worker is mid-Set.
	c.jmu.Lock()
	c.resetJournalLocked()
	c.jmu.Unlock()
	return nil
}

// contentID returns the content identity used to recognize a file regardless of
// its path. It is the same partial/full hash the scanner already trusts for
// exact-duplicate detection, so reusing a fingerprint after a verified match is
// exactly as safe as the tool's own dedup. Returns "" on any read error (which
// then can never equal a stored identity, so the lookup safely misses).
func contentID(path string, size int64) string {
	h, err := computeContentHash(path, size)
	if err != nil {
		return ""
	}
	return h
}

func (c *Cache) Get(path string, size int64, modTime time.Time) (*CacheEntry, bool) {
	modUnix := modTime.UnixNano()
	c.mu.RLock()
	e, ok := c.Entries[path]
	// Exact path hit whose size+mtime still agree — the fast, common case
	// (fresh scans and unchanged re-scans never touch the disk here).
	if ok && e.Size == size && e.ModUnix == modUnix {
		c.mu.RUnlock()
		atomic.AddInt64(&c.hits, 1)
		return e, true
	}
	// Path missed (file moved/renamed, or this path's file changed): look for a
	// candidate with the same size+mtime, then CONFIRM identical bytes via the
	// content hash before trusting it. The confirmation reads only a moved/new
	// file (proportional to changes), never the unchanged bulk of a re-scan.
	cand, cok := c.bySig[sigKey(size, modUnix)]
	c.mu.RUnlock()
	if cok && cand.Quick != "" && contentID(path, size) == cand.Quick {
		atomic.AddInt64(&c.hits, 1)
		return cand, true
	}
	atomic.AddInt64(&c.misses, 1)
	return nil, false
}

func (c *Cache) Set(path string, e *CacheEntry) {
	c.mu.Lock()
	c.Entries[path] = e
	c.bySig[sigKey(e.Size, e.ModUnix)] = e
	c.mu.Unlock()
	// Persist immediately — the data is durable the moment it is computed, not
	// only when the whole scan finishes. O(1): one small append, no re-marshal.
	c.journalAppend(path, e)
}

func (c *Cache) Clear() int {
	c.mu.Lock()
	n := len(c.Entries)
	c.Entries = make(map[string]*CacheEntry)
	c.bySig = make(map[string]*CacheEntry)
	c.mu.Unlock()
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
	os.Remove(cacheFile)
	c.jmu.Lock()
	if c.journal != nil {
		c.resetJournalLocked() // wipe journaled entries, keep the open handle valid
	} else {
		os.Remove(journalFile)
	}
	c.jmu.Unlock()
	return n
}

func (c *Cache) Stats() (entries int, hits, misses int64, fileBytes int64) {
	c.mu.RLock()
	entries = len(c.Entries)
	c.mu.RUnlock()
	hits = atomic.LoadInt64(&c.hits)
	misses = atomic.LoadInt64(&c.misses)
	if st, err := os.Stat(cacheFile); err == nil {
		fileBytes = st.Size()
	}
	return
}

// ── Append-only journal (write-ahead log) ────────────────────────────────
// Started once at startup via OpenJournal; before that, every append is a no-op
// so unit tests and library callers never create files implicitly.

// OpenJournal replays any journal left beside the snapshot — recovering every
// fingerprint computed before an interrupted run could compact it — and then
// opens that journal for appending. Returns the number of entries recovered.
func (c *Cache) OpenJournal(path string) (int, error) {
	c.jmu.Lock()
	defer c.jmu.Unlock()
	c.jpath = path
	recovered := c.replayLocked(path) // best-effort; folds journal over the snapshot
	// Open in append mode, keeping any recovered lines on disk (the snapshot may
	// not yet contain them) until the next compaction folds them in.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return recovered, err
	}
	c.journal = f
	return recovered, nil
}

// replayLocked folds an on-disk journal over the current in-memory map: every
// line is one record. A torn final line — the classic crash-mid-write artifact —
// is skipped, not fatal. Caller holds jmu.
func (c *Cache) replayLocked(path string) (recovered int) {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // entries with VHashes/PSig can be long
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec journalRec
		if json.Unmarshal(line, &rec) != nil || rec.E == nil || rec.P == "" {
			continue // torn or malformed line — skip it
		}
		c.Entries[rec.P] = rec.E
		c.bySig[sigKey(rec.E.Size, rec.E.ModUnix)] = rec.E
		recovered++
	}
	// Distinguish clean EOF from a read error (I/O failure on a NAS/USB drive, or
	// an over-long line): on error the replay is PARTIAL, so warn rather than
	// silently report a complete recovery. The missing fingerprints simply
	// recompute this scan and get re-journaled.
	if err := sc.Err(); err != nil {
		log.Printf("[WARN] Cache journal replay stopped early after %d entries (%v) — the rest will be recomputed", recovered, err)
	}
	return recovered
}

// journalAppend writes one entry to the journal the instant it is computed.
// O(1) — a single small write, no re-serialization of the map. No-op until
// OpenJournal has run, or once a write has failed (logged once).
func (c *Cache) journalAppend(path string, e *CacheEntry) {
	c.jmu.Lock()
	defer c.jmu.Unlock()
	if c.journal == nil || c.jerr {
		return
	}
	line, err := json.Marshal(journalRec{P: path, E: e})
	if err != nil {
		return
	}
	line = append(line, '\n')
	if _, err := c.journal.Write(line); err != nil {
		c.jerr = true
		log.Printf("[WARN] Cache journal write failed (in-memory cache still intact): %v", err)
	}
}

// resetJournalLocked empties the journal, called after a snapshot has absorbed
// every journaled entry. It closes and recreates the file with O_TRUNC rather
// than Truncate(0): on Windows an append-only handle (FILE_APPEND_DATA) cannot
// reliably shrink itself. Caller holds jmu.
func (c *Cache) resetJournalLocked() {
	if c.journal == nil {
		return
	}
	c.journal.Close()
	f, err := os.OpenFile(c.jpath, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		c.journal = nil
		c.jerr = true
		return
	}
	c.journal = f
	c.jerr = false
}

// SyncJournal flushes the journal to stable storage. Called on graceful shutdown
// so even a power loss right after Ctrl-C keeps the last entries.
func (c *Cache) SyncJournal() {
	c.jmu.Lock()
	defer c.jmu.Unlock()
	if c.journal != nil {
		c.journal.Sync()
	}
}

// CloseJournal flushes and releases the journal handle. The on-disk log is left
// fully intact (it is only emptied by Save's compaction), so closing without a
// prior Save leaves exactly the state a crash would — which the next OpenJournal
// recovers. Releasing the handle also lets the file be removed on Windows.
func (c *Cache) CloseJournal() {
	c.jmu.Lock()
	defer c.jmu.Unlock()
	if c.journal != nil {
		c.journal.Sync()
		c.journal.Close()
		c.journal = nil
	}
}

// ─────────────────────────────────────────────────────────────────────
// Session — saved scan results that can be resumed later
// ─────────────────────────────────────────────────────────────────────

type Session struct {
	Version   int               `json:"version"`
	Timestamp time.Time         `json:"timestamp"`
	Settings  ScanRequest       `json:"settings"`
	Groups    []*DuplicateGroup `json:"groups"`
	Selected  []string          `json:"selected"`
	Note      string            `json:"note,omitempty"`
}

const sessionFile = "session.json"
const sessionVersion = 1

// Thumbnail cache directory. Each thumbnail is stored as a JPEG file with
// its name being the SHA1(path|size|modtime|width). This makes cache hits
// O(1) — just read the file. Survives server restarts.
const thumbCacheDir = "thumbs"
const thumbStdSize = 240 // single canonical size — saves duplicate decodes

// thumbSem limits concurrent image decodes across the thumbnail handler and the
// batch generator. The encoders now downscale straight from the decoded image
// (encodeThumbDirect — no full-size NRGBA copy), so peak memory is ~one decode
// buffer per slot; we can therefore run ~¾ of the cores in parallel for much
// faster bulk thumbnailing instead of the old fixed 4.
var thumbSem = make(chan struct{}, thumbDecodeConc())

// thumbDecodeConc sizes thumbSem from the CPU count (clamped 4..16). Capped so a
// burst of multi-second HEIC decodes can't swamp RAM on a smaller machine.
func thumbDecodeConc() int {
	n := runtime.NumCPU() * 3 / 4
	if n < 4 {
		n = 4
	}
	if n > 16 {
		n = 16
	}
	return n
}

var (
	state      = &AppState{}
	procCount  int64
	cancelScan int64 // atomic flag for cancel

	// scanCancelMu guards the in-flight scan's context handles. handleCancel calls
	// scanCancelFn to kill running ffmpeg/ffprobe/fpcalc/decoder subprocesses
	// immediately rather than waiting for them to finish. currentScanCtx() hands
	// that context to subprocess call sites (e.g. external image decoders) not on
	// the explicitly ctx-threaded video/audio fingerprint paths.
	scanCancelMu sync.Mutex
	scanCancelFn context.CancelFunc
	scanCtxCur   context.Context = context.Background()

	cancelThumb   int64 // atomic flag for thumbnail-gen cancel
	thumbCancelMu sync.Mutex
	thumbCancelFn context.CancelFunc

	// Atomic worker counters, snapshotted into state.thumbProgress by a ticker.
	thumbDone    int64
	thumbFailed  int64
	thumbSkipped int64

	cache = NewCache()

	// Decode counters for debugging
	decNative int64
	decExt    int64
	decFail   int64
)

var nativeExts = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true,
	".bmp": true, ".tiff": true, ".tif": true, ".webp": true,
	".heic": true, ".heif": true,
}

var allImgExts = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true,
	".bmp": true, ".tiff": true, ".tif": true, ".webp": true,
	".heic": true, ".heif": true, ".avif": true,
	".cr2": true, ".cr3": true, ".nef": true, ".arw": true,
	".dng": true, ".orf": true, ".rw2": true, ".raf": true,
	".pef": true, ".srw": true, ".x3f": true, ".raw": true,
	".jxl": true, ".ico": true, ".psd": true,
}

// Video extensions
var videoExts = map[string]bool{
	".mp4": true, ".mkv": true, ".avi": true, ".mov": true,
	".wmv": true, ".flv": true, ".webm": true, ".m4v": true,
	".mpg": true, ".mpeg": true, ".3gp": true, ".vob": true,
	".ts": true, ".mts": true, ".m2ts": true, ".divx": true,
	".rmvb": true, ".asf": true, ".ogv": true,
}

// Audio extensions
var audioExts = map[string]bool{
	".mp3": true, ".m4a": true, ".wav": true, ".flac": true,
	".ogg": true, ".aac": true, ".wma": true, ".opus": true,
	".aiff": true, ".aif": true, ".alac": true, ".ape": true,
	".dsd": true, ".dsf": true, ".mid": true, ".midi": true,
	".amr": true, ".au": true, ".caf": true,
}

// Document extensions
var docExts = map[string]bool{
	".pdf": true, ".doc": true, ".docx": true, ".rtf": true,
	".odt": true, ".txt": true, ".md": true,
	".xls": true, ".xlsx": true, ".csv": true, ".ods": true, ".tsv": true,
	".ppt": true, ".pptx": true, ".odp": true, ".key": true,
	".epub": true, ".mobi": true, ".azw": true, ".azw3": true, ".djvu": true,
	".html": true, ".htm": true, ".xml": true, ".json": true,
}

// Archive extensions
var archiveExts = map[string]bool{
	".zip": true, ".rar": true, ".7z": true, ".tar": true,
	".gz": true, ".bz2": true, ".xz": true, ".tgz": true,
	".tbz2": true, ".lzma": true, ".cab": true, ".iso": true, ".dmg": true,
}

// extsForCategory returns the extension set for a given file category.
func extsForCategory(category string) map[string]bool {
	switch category {
	case "videos":
		return videoExts
	case "audio":
		return audioExts
	case "documents":
		return docExts
	case "archives":
		return archiveExts
	case "all":
		all := make(map[string]bool, 200)
		for k := range allImgExts {
			all[k] = true
		}
		for k := range videoExts {
			all[k] = true
		}
		for k := range audioExts {
			all[k] = true
		}
		for k := range docExts {
			all[k] = true
		}
		for k := range archiveExts {
			all[k] = true
		}
		return all
	case "images":
		fallthrough
	default:
		return allImgExts
	}
}

// isImageCategory returns true if the category supports perceptual matching
// via image decode + dHash/aHash/pHash.
func isImageCategory(category string) bool {
	return category == "" || category == "images"
}

// Partial-hash threshold. Files larger than this use a 3-chunk partial hash
// (first + middle + last 64KB) instead of full MD5. Massively faster for
// large videos/archives. Size is mixed into the hash to prevent cross-size
// false positives.
const partialHashThreshold = 1 * 1024 * 1024 // 1 MB
const partialChunkSize = 64 * 1024           // 64 KB

// computeContentHash returns the MD5 hex of a file's content. For files
// larger than partialHashThreshold, only the first/middle/last 64KB chunks
// are read (much faster for videos/archives). The file size is mixed into
// the hash so two files of different sizes can never collide.
func computeContentHash(path string, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()

	if size <= partialHashThreshold {
		// Whole-file hash for small files (≤1 MB)
		if _, err := io.Copy(h, f); err != nil {
			return "", err
		}
	} else {
		// Three-chunk partial hash + size mixed in
		buf := make([]byte, partialChunkSize)

		if _, err := io.ReadFull(f, buf); err != nil {
			return "", err
		}
		h.Write(buf)

		mid := size/2 - int64(partialChunkSize)/2
		if _, err := f.Seek(mid, io.SeekStart); err != nil {
			return "", err
		}
		if _, err := io.ReadFull(f, buf); err != nil {
			return "", err
		}
		h.Write(buf)

		if _, err := f.Seek(-int64(partialChunkSize), io.SeekEnd); err != nil {
			return "", err
		}
		if _, err := io.ReadFull(f, buf); err != nil {
			return "", err
		}
		h.Write(buf)

		var sb [8]byte
		s := uint64(size)
		for i := 0; i < 8; i++ {
			sb[i] = byte(s >> (i * 8))
		}
		h.Write(sb[:])
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// ─────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────

func Main() {
	initLogger()
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()

	listenArg := ""
	if len(os.Args) > 1 {
		listenArg = os.Args[1]
	}

	logf("INFO", "DupCleaner starting on Go %s, GOOS=%s, NumCPU=%d",
		runtime.Version(), runtime.GOOS, runtime.NumCPU())
	logf("INFO", "Native decoders: jpg png gif bmp tiff webp heic heif")
	logf("INFO", "External fallback: avif raw psd jxl ico (needs sips/magick/ffmpeg)")

	// Load cache from disk if it exists
	if err := cache.Load(cacheFile); err != nil {
		if !os.IsNotExist(err) {
			logf("WARN", "Cache load failed (will start fresh): %v", err)
		} else {
			logf("INFO", "No existing cache found, will create one")
		}
	} else {
		ent, _, _, _ := cache.Stats()
		logf("INFO", "Loaded cache: %d entries", ent)
	}
	// Replay the write-ahead journal on top of the snapshot — this recovers any
	// fingerprints computed by a previous run that was interrupted before it could
	// compact — then open the journal for this run's incremental saves.
	if recovered, err := cache.OpenJournal(journalFile); err != nil {
		logf("WARN", "Cache journal unavailable (incremental save disabled): %v", err)
	} else if recovered > 0 {
		logf("INFO", "Recovered %d cache entries from an interrupted scan's journal", recovered)
	}
	// Flush the journal to stable storage on Ctrl-C / termination so the last few
	// entries survive even a power loss right at shutdown.
	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
		<-sigc
		logf("INFO", "Shutting down — flushing cache journal")
		cache.SyncJournal()
		os.Exit(0)
	}()
	loadToolsConfig()
	loadRules()

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/api/scan", handleScan)
	mux.HandleFunc("/api/scan/cancel", handleCancel)
	mux.HandleFunc("/api/progress", handleProgress)
	mux.HandleFunc("/api/results", handleResults)
	mux.HandleFunc("/api/preview", handlePreview)
	mux.HandleFunc("/api/thumbnail", handleThumbnail)
	mux.HandleFunc("/api/thumbnails/generate", handleThumbGenStart)
	mux.HandleFunc("/api/thumbnails/progress", handleThumbProgress)
	mux.HandleFunc("/api/thumbnails/cancel", handleThumbCancel)
	mux.HandleFunc("/api/delete", handleDelete)
	mux.HandleFunc("/api/restore", handleRestore)
	mux.HandleFunc("/api/open", handleOpen)
	mux.HandleFunc("/api/open-file", handleOpenFile)
	mux.HandleFunc("/api/export", handleExport)
	mux.HandleFunc("/api/smart-select", handleSmartSelect)
	mux.HandleFunc("/api/log", handleClientLog)
	mux.HandleFunc("/api/logs", handleViewLogs)
	mux.HandleFunc("/api/health", handleHealth)
	mux.HandleFunc("/api/system", handleSystem)
	mux.HandleFunc("/api/cache/stats", handleCacheStats)
	mux.HandleFunc("/api/cache/clear", handleCacheClear)
	mux.HandleFunc("/api/session/save", handleSessionSave)
	mux.HandleFunc("/api/session/load", handleSessionLoad)
	mux.HandleFunc("/api/session/exists", handleSessionExists)
	mux.HandleFunc("/api/session/clear", handleSessionClear)
	mux.HandleFunc("/api/folders/common", handleCommonFolders)
	mux.HandleFunc("/api/folder/pick", handleFolderPick)
	mux.HandleFunc("/api/file/pick", handleFilePick)
	mux.HandleFunc("/api/video/tools", handleVideoTools)
	mux.HandleFunc("/api/audio/tools", handleAudioTools)
	mux.HandleFunc("/api/tools/download", handleToolDownload)
	mux.HandleFunc("/api/rules", handleListRules)
	mux.HandleFunc("/api/rules/save", handleSaveRule)
	mux.HandleFunc("/api/rules/delete", handleDeleteRule)
	mux.HandleFunc("/api/rules/apply", handleApplyRule)

	addr, browseURL, exposed := resolveListenAddr(listenArg)
	logf("INFO", "Listening on http://%s", addr)
	if exposed {
		logf("WARN", "Bound to %s — reachable from OTHER machines on your network. "+
			"This server can delete and read local files; only expose it on a trusted network.", addr)
	}
	logf("INFO", "Logs writing to: dupcleaner.log (in current directory)")

	go func() {
		time.Sleep(700 * time.Millisecond)
		openBrowser(browseURL)
	}()

	if err := http.ListenAndServe(addr, mux); err != nil {
		logf("FATAL", "Server failed: %v", err)
		os.Exit(1)
	}
}

// serverHost returns the default listen host when the CLI arg gives only a port.
// Loopback-only (127.0.0.1) for safety; set DUPCLEANER_HOST=0.0.0.0 to bind all
// interfaces without passing a host:port arg (e.g. Docker).
func serverHost() string {
	if h := os.Getenv("DUPCLEANER_HOST"); h != "" {
		return h
	}
	return "127.0.0.1"
}

// resolveListenAddr turns the optional first CLI argument into a bind address.
// It accepts either a bare port ("8030" → bound to serverHost(), i.e. loopback
// unless DUPCLEANER_HOST overrides) or a full host:port ("0.0.0.0:8030",
// "192.168.1.5:8030", "[::1]:8030") which is bound verbatim. Returns the bind
// address, a browser-friendly URL (0.0.0.0/:: rewritten to localhost so the
// local browser actually opens), and whether the bind reaches beyond loopback.
func resolveListenAddr(arg string) (addr, browseURL string, exposed bool) {
	host, port := serverHost(), "7891"
	if arg != "" {
		if strings.Contains(arg, ":") {
			if h, p, err := net.SplitHostPort(arg); err == nil { // "host:port" / "[::1]:port" / ":port"
				if h != "" {
					host = h
				} else {
					host = "0.0.0.0" // ":8030" → all interfaces
				}
				port = p
			} else {
				port = arg // malformed; let ListenAndServe surface the error
			}
		} else {
			port = arg // bare port
		}
	}
	addr = net.JoinHostPort(host, port)
	exposed = !(host == "127.0.0.1" || host == "::1" || strings.EqualFold(host, "localhost"))
	bh := host
	if host == "0.0.0.0" || host == "::" || host == "" {
		bh = "127.0.0.1" // these aren't usable client addresses — open loopback
	}
	browseURL = "http://" + net.JoinHostPort(bh, port)
	return
}

func openBrowser(url string) {
	switch runtime.GOOS {
	case "windows":
		exec.Command("cmd", "/c", "start", url).Start()
	case "darwin":
		exec.Command("open", url).Start()
	default:
		exec.Command("xdg-open", url).Start()
	}
}

// ─────────────────────────────────────────────────────────────────────
// HTTP Handlers
// ─────────────────────────────────────────────────────────────────────

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	// Try disk first (allows live editing — just refresh browser). Check the
	// CWD and the package source path so `go run .` from the repo root still
	// picks up edits to internal/dup/index.html.
	for _, p := range []string{"index.html", filepath.Join("internal", "dup", "index.html")} {
		if data, err := os.ReadFile(p); err == nil {
			logf("DEBUG", "Serving %s from disk (%d bytes)", p, len(data))
			w.Write(data)
			return
		}
	}
	// Fall back to embedded
	logf("DEBUG", "Serving embedded index.html (%d bytes)", len(embeddedHTML))
	w.Write(embeddedHTML)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":          true,
		"go_version":  runtime.Version(),
		"os":          runtime.GOOS,
		"num_cpu":     runtime.NumCPU(),
		"native_exts": getKeys(nativeExts),
	})
}

// handleSystem returns machine info so the UI can build sensible defaults
func handleSystem(w http.ResponseWriter, r *http.Request) {
	ram := physicalRAMBytes()
	numCPU := runtime.NumCPU()
	// Recommended workers: leave ~25% CPU headroom so the machine stays
	// responsive during a scan (running every logical CPU flat-out makes the
	// UI lag). RAM estimate per image ~48MB (decoded 24MP buffer; no NRGBA copy)
	// caps it on low-RAM machines. "Full CPU" (numCPU) stays available as an explicit max-speed choice.
	const perImageMB = 48
	maxByRAM := int(ram / 2 / (perImageMB * 1024 * 1024))
	if maxByRAM < 1 {
		maxByRAM = 1
	}
	recommended := numCPU * 3 / 4
	if recommended < 1 {
		recommended = 1
	}
	if recommended > maxByRAM {
		recommended = maxByRAM
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"num_cpu":      numCPU,
		"ram_gb":       float64(ram) / (1024 * 1024 * 1024),
		"ram_bytes":    ram,
		"recommended":  recommended,
		"per_image_mb": perImageMB,
		"max_by_ram":   maxByRAM,
	})
}

func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func handleClientLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var req LogRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	level := strings.ToUpper(req.Level)
	if level == "" {
		level = "INFO"
	}
	loc := ""
	if req.File != "" {
		loc = fmt.Sprintf(" (%s:%d)", req.File, req.Line)
	}
	logf("CLIENT-"+level, "%s%s", req.Msg, loc)
	if req.Stack != "" {
		for _, ln := range strings.Split(req.Stack, "\n") {
			ln = strings.TrimSpace(ln)
			if ln != "" {
				logf("CLIENT-STACK", "  %s", ln)
			}
		}
	}
	w.WriteHeader(204)
}

func handleViewLogs(w http.ResponseWriter, r *http.Request) {
	recentLogMu.Lock()
	lines := append([]string(nil), recentLogs...)
	recentLogMu.Unlock()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	for _, ln := range lines {
		w.Write([]byte(ln))
		w.Write([]byte("\n"))
	}
}

func handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var req ScanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logf("ERROR", "Scan decode failed: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	logf("INFO", "Scan request: mode=%s dirs=%v threshold=%d skipHidden=%v minKB=%d useCache=%v",
		req.Mode, req.Dirs, req.Threshold, req.SkipHidden, req.MinSizeKB, req.UseCache)

	valid := make([]string, 0)
	for _, d := range req.Dirs {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}
		if _, err := os.Stat(d); err != nil {
			logf("ERROR", "Directory not found: %s", d)
			http.Error(w, "Directory not found: "+d, 400)
			return
		}
		valid = append(valid, d)
	}
	if len(valid) == 0 {
		http.Error(w, "no valid dirs", 400)
		return
	}
	req.Dirs = valid

	// Threshold range is mode-dependent: image/video perceptual modes use a
	// Hamming distance (0–20); audio acoustic mode uses a similarity percent
	// (0–100). Clamping audio to 20 would force ~0.20 similarity and match
	// almost everything, so widen the ceiling for acoustic mode.
	maxThreshold := 20
	if req.Mode == "acoustic" {
		maxThreshold = 100
	}
	if req.Threshold < 0 {
		req.Threshold = 0
	}
	if req.Threshold > maxThreshold {
		req.Threshold = maxThreshold
	}
	if req.Mode == "" {
		req.Mode = "quick"
	}

	state.mu.Lock()
	if state.scanning {
		state.mu.Unlock()
		http.Error(w, "scan in progress", 409)
		return
	}
	if state.thumbing {
		state.mu.Unlock()
		http.Error(w, "thumbnail generation in progress", 409)
		return
	}
	state.scanning = true
	state.progress = ScanProgress{Status: "Initializing..."}
	state.groups = nil
	state.undo = nil // a new scan invalidates any pending undo from the old result set
	state.mu.Unlock()

	atomic.StoreInt64(&decNative, 0)
	atomic.StoreInt64(&decExt, 0)
	atomic.StoreInt64(&decFail, 0)

	go runScan(req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func handleProgress(w http.ResponseWriter, r *http.Request) {
	state.mu.RLock()
	p := state.progress
	state.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	json.NewEncoder(w).Encode(p)
}

func handleThumbGenStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	state.mu.Lock()
	if state.scanning || state.thumbing {
		state.mu.Unlock()
		http.Error(w, "busy", 409)
		return
	}
	if len(state.groups) == 0 {
		state.mu.Unlock()
		http.Error(w, "no results", 400)
		return
	}
	targets := collectThumbTargets(state.groups)
	if len(targets) == 0 {
		state.mu.Unlock()
		http.Error(w, "no image or video files", 400)
		return
	}
	state.thumbing = true
	state.thumbProgress = ThumbProgress{Total: int64(len(targets)), Status: "Starting…", Running: true}
	state.mu.Unlock()

	atomic.StoreInt64(&cancelThumb, 0)
	go runThumbGen()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "started", "total": len(targets)})
}

func handleThumbProgress(w http.ResponseWriter, r *http.Request) {
	state.mu.RLock()
	p := state.thumbProgress
	state.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	json.NewEncoder(w).Encode(p)
}

func handleThumbCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	atomic.StoreInt64(&cancelThumb, 1)
	thumbCancelMu.Lock()
	if thumbCancelFn != nil {
		thumbCancelFn()
	}
	thumbCancelMu.Unlock()
	logf("INFO", "Thumbnail generation cancel requested")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"cancelled": true})
}

func handleResults(w http.ResponseWriter, r *http.Request) {
	// Copy the slice header under lock so we hold a stable reference.
	// The slice elements are []*DuplicateGroup — since delete operations
	// replace the whole slice rather than mutating elements, this is safe.
	state.mu.RLock()
	groups := make([]*DuplicateGroup, len(state.groups))
	copy(groups, state.groups)
	state.mu.RUnlock()

	logf("DEBUG", "Results request: returning %d groups", len(groups))

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"groups": groups,
		"total":  len(groups),
	})
}

func handlePreview(w http.ResponseWriter, r *http.Request) {
	if isCrossSite(r) {
		http.Error(w, "forbidden", 403)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path required", 400)
		return
	}
	// Only stream files the scanner found — never arbitrary local files.
	if !pathInResults(path) {
		logf("WARN", "Preview rejected (not in scan results): %s", path)
		http.Error(w, "not a scanned file", 403)
		return
	}
	if _, err := os.Stat(path); err != nil {
		logf("ERROR", "Preview not found: %s", path)
		http.Error(w, "not found", 404)
		return
	}

	ext := strings.ToLower(filepath.Ext(path))

	// Videos: serve raw with proper MIME so browser <video> can play
	if videoExts[ext] {
		ct := "video/mp4"
		switch ext {
		case ".webm":
			ct = "video/webm"
		case ".mov":
			ct = "video/quicktime"
		case ".avi":
			ct = "video/x-msvideo"
		case ".mkv":
			ct = "video/x-matroska"
		case ".ogv":
			ct = "video/ogg"
		}
		w.Header().Set("Content-Type", ct)
		http.ServeFile(w, r, path)
		return
	}

	// Audio: serve raw
	if audioExts[ext] {
		http.ServeFile(w, r, path)
		return
	}

	// Documents/archives: return a download
	if docExts[ext] || archiveExts[ext] {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment; filename=\""+filepath.Base(path)+"\"")
		http.ServeFile(w, r, path)
		return
	}

	// For non-HEIC native image formats, serve raw
	if nativeExts[ext] && ext != ".heic" && ext != ".heif" {
		f, err := os.Open(path)
		if err != nil {
			http.Error(w, "read error", 500)
			return
		}
		defer f.Close()
		buf := make([]byte, 512)
		n, _ := f.Read(buf)
		ct := http.DetectContentType(buf[:n])
		f.Seek(0, 0)
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Cache-Control", "max-age=3600")
		io.Copy(w, f)
		return
	}

	// Decode HEIC/AVIF/RAW etc. to JPEG
	thumbSem <- struct{}{} // limit concurrent full-image decodes
	img, _, err := decodeImageFile(path)
	if err != nil {
		<-thumbSem
		logf("ERROR", "Preview decode failed: %s: %v", path, err)
		http.Error(w, "decode failed: "+err.Error(), 500)
		return
	}
	<-thumbSem
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "max-age=3600")
	if err := jpeg.Encode(w, img, &jpeg.Options{Quality: 88}); err != nil {
		// Headers are already sent, so we can't change the status — just log.
		logf("ERROR", "Preview JPEG encode failed: %s: %v", path, err)
	}
}

// thumbCachePath returns the deterministic disk path for a given source file's
// thumbnail. The key encodes path+size+mtime so cached thumbnails are
// auto-invalidated when the source file changes.
func thumbCachePath(srcPath string, size int64, modUnix int64) string {
	key := fmt.Sprintf("%s|%d|%d|%d", srcPath, size, modUnix, thumbStdSize)
	h := sha1.Sum([]byte(key))
	hex := fmt.Sprintf("%x", h)
	// Spread across 256 sub-folders to avoid one giant flat directory
	return filepath.Join(thumbCacheDir, hex[:2], hex+".jpg")
}

// encodeImageThumb decodes a source image and returns the JPEG-encoded thumbnail
// bytes. It bounds concurrency with thumbSem and downscales via encodeThumbDirect
// (sampling straight from the decoded image — no full-size NRGBA copy), so each
// slot holds ~one decode buffer instead of ~96 MB. Shared by the live thumbnail
// handler and the batch generator; produces the same thumbnails as the scan's
// piggyback path.
func encodeImageThumb(path string) ([]byte, error) {
	thumbSem <- struct{}{}
	defer func() { <-thumbSem }()
	img, _, err := decodeImageFile(path)
	if err != nil {
		return nil, err
	}
	return encodeThumbDirect(img)
}

// encodeThumbDirect resizes an ALREADY-DECODED image to fit thumbStdSize and
// returns the JPEG-encoded thumbnail bytes, sampling source pixels directly
// (nearest-neighbour, identical math to resizeFitFast) WITHOUT allocating a
// full-resolution NRGBA copy. The scan's decode workers call this on the image
// they just decoded for hashing, so a thumbnail costs only the cheap downscale —
// not a second full decode — while the per-image RAM gate (which assumes no
// extra full-size buffer) still holds.
func encodeThumbDirect(src image.Image) ([]byte, error) {
	b := src.Bounds()
	w, h := b.Dx(), b.Dy()
	if w == 0 || h == 0 {
		return nil, fmt.Errorf("empty image")
	}
	nw, nh := w, h
	if w > thumbStdSize || h > thumbStdSize {
		scale := math.Min(float64(thumbStdSize)/float64(w), float64(thumbStdSize)/float64(h))
		nw = int(math.Round(float64(w) * scale))
		nh = int(math.Round(float64(h) * scale))
		if nw < 1 {
			nw = 1
		}
		if nh < 1 {
			nh = 1
		}
	}
	dst := image.NewNRGBA(image.Rect(0, 0, nw, nh))
	stepX := (w << 16) / nw
	stepY := (h << 16) / nh
	for y := 0; y < nh; y++ {
		srcY := (y*stepY + 0x8000) >> 16
		if srcY >= h {
			srcY = h - 1
		}
		dstRow := y * dst.Stride
		for x := 0; x < nw; x++ {
			srcX := (x*stepX + 0x8000) >> 16
			if srcX >= w {
				srcX = w - 1
			}
			// NRGBAModel un-premultiplies, matching resizeFitFast→toNRGBA output.
			c := color.NRGBAModel.Convert(src.At(b.Min.X+srcX, b.Min.Y+srcY)).(color.NRGBA)
			di := dstRow + x*4
			dst.Pix[di] = c.R
			dst.Pix[di+1] = c.G
			dst.Pix[di+2] = c.B
			dst.Pix[di+3] = c.A
		}
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 80}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeThumbAtomic writes thumbnail bytes to dst via a temp file + rename, so a
// concurrent reader never sees a half-written file.
func writeThumbAtomic(dst string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// ensureThumbCacheDir creates the thumbs directory if it doesn't exist.
// Called lazily from the thumbnail handler.
var thumbDirOnce sync.Once

func ensureThumbCacheDir() {
	thumbDirOnce.Do(func() {
		if err := os.MkdirAll(thumbCacheDir, 0755); err != nil {
			logf("WARN", "Could not create thumbs directory: %v", err)
		}
	})
}

// serveCachedThumb serves an already-generated thumbnail/poster for path if one
// exists on disk, returning true if it wrote a response. Used for video posters
// (images keep their existing inline fast path).
func serveCachedThumb(w http.ResponseWriter, r *http.Request, path string) bool {
	stat, err := os.Stat(path)
	if err != nil {
		return false
	}
	cachePath := thumbCachePath(path, stat.Size(), stat.ModTime().UnixNano())
	cf, err := os.Open(cachePath)
	if err != nil {
		return false
	}
	defer cf.Close()
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "max-age=86400")
	http.ServeContent(w, r, "thumb.jpg", stat.ModTime(), cf)
	return true
}

func handleThumbnail(w http.ResponseWriter, r *http.Request) {
	if isCrossSite(r) {
		http.Error(w, "forbidden", 403)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path required", 400)
		return
	}
	// Only render thumbnails for files the scanner found — never arbitrary files.
	if !pathInResults(path) {
		logf("WARN", "Thumbnail rejected (not in scan results): %s", path)
		http.Error(w, "not a scanned file", 403)
		return
	}
	ext := strings.ToLower(filepath.Ext(path))
	light := r.URL.Query().Get("theme") == "light"

	// Non-image files. A video may have a generated frame-poster on disk; serve
	// it if present, otherwise fall back to the file-type icon. Other categories
	// always get the icon.
	if !allImgExts[ext] {
		if videoExts[ext] && serveCachedThumb(w, r, path) {
			return
		}
		serveTypeIcon(w, ext, true, light)
		return
	}

	// Get source file metadata for cache key — fast (single stat call)
	stat, err := os.Stat(path)
	if err != nil {
		// Source missing — return non-cached fallback icon
		serveTypeIcon(w, ext, false, light)
		return
	}

	ensureThumbCacheDir()
	cachePath := thumbCachePath(path, stat.Size(), stat.ModTime().UnixNano())

	// FAST PATH: thumbnail exists on disk. Just serve it. Sub-millisecond.
	if cf, err := os.Open(cachePath); err == nil {
		defer cf.Close()
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Cache-Control", "max-age=86400")
		// Use http.ServeContent so range requests + ETag work for free
		http.ServeContent(w, r, "thumb.jpg", stat.ModTime(), cf)
		return
	}

	// SLOW PATH: decode source image, resize, encode JPEG, save to disk.
	data, derr := encodeImageThumb(path)
	if derr != nil {
		logf("DEBUG", "Thumbnail decode failed: %s: %v", filepath.Base(path), derr)
		serveTypeIcon(w, ext, false, light)
		return
	}

	// Save to disk (best-effort — don't block the client if disk is slow/full).
	go func(d []byte, dst string) {
		if err := writeThumbAtomic(dst, d); err != nil {
			logf("DEBUG", "Thumbnail cache write failed: %v", err)
		}
	}(append([]byte(nil), data...), cachePath)

	// Serve the JPEG bytes we just encoded.
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "max-age=86400")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Write(data)
}

// serveTypeIcon returns an SVG icon for the file's category. If cacheable=true
// the response is cached for a day; otherwise the response is non-cacheable
// (used as a fallback when image decode fails).
func serveTypeIcon(w http.ResponseWriter, ext string, cacheable bool, light bool) {
	icon := "📄"
	bg := "#212735"
	color := "#9ca3af"
	label := strings.ToUpper(strings.TrimPrefix(ext, "."))
	switch {
	case videoExts[ext]:
		icon = "🎬"
		bg, color = "#1e1b4b", "#a5b4fc"
		if light {
			bg, color = "#e0e7ff", "#4f46e5"
		}
	case audioExts[ext]:
		icon = "🎵"
		bg, color = "#1e2937", "#86efac"
		if light {
			bg, color = "#d1fae5", "#047857"
		}
	case docExts[ext]:
		icon = "📄"
		bg, color = "#1f2937", "#fde68a"
		if light {
			bg, color = "#fef3c7", "#b45309"
		}
	case archiveExts[ext]:
		icon = "📦"
		bg, color = "#2d1b1b", "#fca5a5"
		if light {
			bg, color = "#fee2e2", "#b91c1c"
		}
	case allImgExts[ext]:
		icon = "🖼️"
		bg, color = "#1f1f2b", "#fca5a5"
		if light {
			bg, color = "#eef0f4", "#b91c1c"
		}
	default:
		if light {
			bg, color = "#eef0f4", "#4b5563"
		}
	}
	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 180" width="240" height="180">
<rect width="240" height="180" fill="%s"/>
<text x="120" y="95" text-anchor="middle" font-size="72" font-family="sans-serif">%s</text>
<text x="120" y="148" text-anchor="middle" font-size="18" font-weight="700" font-family="sans-serif" fill="%s">%s</text>
</svg>`, bg, icon, color, label)
	w.Header().Set("Content-Type", "image/svg+xml")
	if cacheable {
		w.Header().Set("Cache-Control", "max-age=86400")
	} else {
		w.Header().Set("Cache-Control", "no-cache, no-store")
	}
	w.Write([]byte(svg))
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	// CSRF guard: deletion is the only file-mutating endpoint, so a cross-site
	// page must never be able to trigger it. Our own UI sends same-origin.
	if isCrossSite(r) {
		http.Error(w, "forbidden", 403)
		return
	}
	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Only delete files the current scan actually found — never arbitrary paths.
	// This mirrors the allowlist the open/launch endpoints already enforce.
	allowed := make([]string, 0, len(req.Paths))
	for _, p := range req.Paths {
		if pathInResults(p) {
			allowed = append(allowed, p)
		} else {
			logf("WARN", "Delete rejected (not in scan results): %s", p)
		}
	}
	// Never wipe an entire duplicate group — always retain one copy.
	allowed = keepOnePerGroup(allowed)

	logf("INFO", "Delete request: %d paths (%d allowed), toTrash=%v", len(req.Paths), len(allowed), req.ToTrash)
	t0 := time.Now()
	deleted, failed := batchDelete(allowed, req.ToTrash)
	logf("INFO", "Delete done in %v: %d ok, %d failed", time.Since(t0), len(deleted), len(failed))

	if len(deleted) > 0 {
		delSet := make(map[string]bool, len(deleted))
		for _, d := range deleted {
			delSet[d] = true
		}

		state.mu.Lock()
		// Snapshot the results BEFORE pruning so a to-trash delete can be undone in
		// one click. The old *DuplicateGroup pointers are never mutated (we build
		// fresh g2 copies below), so the snapshot stays a faithful pre-delete view.
		prevGroups := state.groups
		prevWasted := state.progress.WastedMB
		prevGroupN := state.progress.Groups

		newGroups := make([]*DuplicateGroup, 0, len(state.groups))
		for _, g := range state.groups {
			pruned := make([]*ImageInfo, 0, len(g.Images))
			for _, img := range g.Images {
				if !delSet[img.Path] {
					pruned = append(pruned, img)
				}
			}
			if len(pruned) < 2 {
				continue
			}
			g2 := *g
			g2.Images = pruned
			var total int64
			for _, img := range pruned {
				total += img.Size
			}
			g2.TotalSize = total
			g2.WastedSize = total - largestSize(pruned)
			if g2.WastedSize < 0 {
				g2.WastedSize = 0
			}
			newGroups = append(newGroups, &g2)
		}
		state.groups = newGroups
		var tw int64
		for _, g := range state.groups {
			tw += g.WastedSize
		}
		state.progress.WastedMB = float64(tw) / 1024 / 1024
		state.progress.Groups = len(state.groups)
		// Record the undo only for recoverable (to-trash) deletes; a permanent
		// delete clears any stale undo so the UI never offers a restore that can't work.
		if req.ToTrash {
			state.undo = &undoRecord{paths: deleted, groups: prevGroups, wastedMB: prevWasted, groupN: prevGroupN}
		} else {
			state.undo = nil
		}
		state.mu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"deleted":  deleted,
		"failed":   failed,
		"can_undo": req.ToTrash && len(deleted) > 0,
	})
}

// handleRestore reverses the most recent move-to-trash deletion: it pulls the
// trashed files back out of the Recycle Bin and restores the pre-delete results
// snapshot so the grid looks exactly as it did before. One-shot — the undo
// record is consumed (and is also cleared by any new scan or permanent delete).
func handleRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	if isCrossSite(r) {
		http.Error(w, "forbidden", 403)
		return
	}
	state.mu.RLock()
	u := state.undo
	state.mu.RUnlock()
	if u == nil || len(u.paths) == 0 {
		http.Error(w, "nothing to undo", 400)
		return
	}

	restored, failed := restoreFromTrash(u.paths)
	logf("INFO", "Restore request: %d trashed paths → %d restored, %d failed", len(u.paths), len(restored), len(failed))

	if len(restored) > 0 {
		state.mu.Lock()
		// Only roll back to the snapshot if it's still the same undo we read (no
		// scan/delete raced in between), so we never resurrect a stale result set.
		if state.undo == u {
			// Restore the pre-delete grid ONLY when every file actually came back;
			// a partial restore would otherwise show "ghost" entries for files that
			// are still in the Recycle Bin. On a partial restore we leave the grid
			// in its post-delete state (the recovered files are on disk and the
			// `failed` list is returned so the UI can warn).
			if len(failed) == 0 {
				state.groups = u.groups
				state.progress.WastedMB = u.wastedMB
				state.progress.Groups = u.groupN
			}
			state.undo = nil // consumed either way — a partial restore isn't safely retryable
		}
		state.mu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"restored": restored,
		"failed":   failed,
	})
}

// restoreFromTrash pulls files back out of the OS trash by their ORIGINAL path.
// On Windows it drives the Recycle Bin via Shell.Application: it walks the bin,
// matches each item's "Original Location" + name against the wanted paths, and
// invokes the localized Restore verb. Returns the paths it got back plus reasons
// for any it couldn't.
func restoreFromTrash(paths []string) (restored []string, failed map[string]string) {
	failed = make(map[string]string)
	if len(paths) == 0 {
		return
	}
	if runtime.GOOS == "windows" {
		var sb strings.Builder
		sb.WriteString("$sh=New-Object -ComObject Shell.Application;$bin=$sh.Namespace(0xA);$want=@(")
		for i, p := range paths {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("'")
			sb.WriteString(strings.ReplaceAll(p, "'", "''"))
			sb.WriteString("'")
		}
		sb.WriteString(");foreach($it in @($bin.Items())){$o=$bin.GetDetailsOf($it,1);$full=Join-Path $o $it.Name;")
		sb.WriteString("if($want -icontains $full){$v=$it.Verbs()|?{$_.Name -match 'estore|ndelete|Wiederherstell|staur|ripristin|herstel'}|select -First 1;")
		sb.WriteString("if($v){$v.DoIt();Write-Host \"OK:$full\"}else{Write-Host \"ERR:$full\"}}}")
		out, err := exec.Command("powershell", "-NoProfile", "-Command", sb.String()).Output()
		if err != nil {
			logf("ERROR", "Restore PowerShell failed: %v", err)
		}
		okSet := make(map[string]bool)
		for _, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "OK:") {
				okSet[strings.TrimPrefix(line, "OK:")] = true
			}
		}
		for _, p := range paths {
			if okSet[p] {
				restored = append(restored, p)
			} else if _, err := os.Stat(p); err == nil {
				restored = append(restored, p) // already back on disk
			} else {
				failed[p] = "not found in Recycle Bin"
			}
		}
		return
	}
	// macOS/Linux: programmatic trash-restore isn't reliably available; the files
	// are in the OS trash and can be restored by hand.
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			restored = append(restored, p)
		} else {
			failed[p] = "undo not supported on this OS — restore from Trash manually"
		}
	}
	return
}

func handleOpen(w http.ResponseWriter, r *http.Request) {
	if isCrossSite(r) {
		http.Error(w, "forbidden", 403)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path required", 400)
		return
	}
	// Only reveal files the scanner found — never arbitrary paths.
	if !pathInResults(path) {
		logf("WARN", "Open-in-folder rejected (not in scan results): %s", path)
		http.Error(w, "not a scanned file", 403)
		return
	}
	logf("DEBUG", "Open in folder: %s", path)
	switch runtime.GOOS {
	case "windows":
		exec.Command("explorer", "/select,", filepath.FromSlash(path)).Start()
	case "darwin":
		exec.Command("open", "-R", path).Start()
	default:
		exec.Command("xdg-open", filepath.Dir(path)).Start()
	}
	w.WriteHeader(200)
}

// dangerousExts are file types we refuse to hand to the OS launcher even when
// they appear in scan results — opening them could execute code.
var dangerousExts = map[string]bool{
	".exe": true, ".bat": true, ".cmd": true, ".com": true, ".scr": true,
	".ps1": true, ".psm1": true, ".vbs": true, ".vbe": true, ".js": true,
	".jse": true, ".wsf": true, ".wsh": true, ".msi": true, ".msp": true,
	".hta": true, ".cpl": true, ".jar": true, ".lnk": true, ".pif": true,
	".reg": true, ".sh": true, ".app": true, ".command": true,
}

// pathInResults reports whether path is one of the files discovered by the
// current scan. The open/launch endpoint uses it so neither a crafted request
// nor a CSRF call from another browser tab can make the local server open an
// arbitrary file on disk.
func pathInResults(path string) bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	for _, g := range state.groups {
		for _, img := range g.Images {
			if img.Path == path {
				return true
			}
		}
	}
	return false
}

// isCrossSite reports whether the request originated from a different site,
// using the Sec-Fetch-Site hint modern browsers attach. It is a CSRF guard for
// endpoints that mutate files or stream local content. (Absent header → treated
// as same-site, for older browsers; the scan-results allowlist is the real gate.)
func isCrossSite(r *http.Request) bool {
	return r.Header.Get("Sec-Fetch-Site") == "cross-site"
}

// keepOnePerGroup filters a delete request so at least one member of every
// duplicate group survives. Without this guard a buggy or malicious request that
// listed every path in a group would delete all copies — total data loss rather
// than just removing redundant duplicates. The spared file is the group's
// index-0 image, which is the oldest (groups are sorted oldest-first) and the
// one the UI and CSV export label "keep".
func keepOnePerGroup(paths []string) []string {
	del := make(map[string]bool, len(paths))
	for _, p := range paths {
		del[p] = true
	}
	state.mu.RLock()
	for _, g := range state.groups {
		if len(g.Images) == 0 {
			continue
		}
		marked := 0
		for _, img := range g.Images {
			if del[img.Path] {
				marked++
			}
		}
		if marked == len(g.Images) {
			delete(del, g.Images[0].Path)
			logf("WARN", "Delete guard: keeping %s to avoid removing every copy in group %d", g.Images[0].Path, g.ID)
		}
	}
	state.mu.RUnlock()
	// Preserve the caller's original ordering.
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if del[p] {
			out = append(out, p)
		}
	}
	return out
}

func handleOpenFile(w http.ResponseWriter, r *http.Request) {
	// CSRF guard: a cross-site page must not be able to make the local server
	// launch files. Our own UI sends Sec-Fetch-Site: same-origin; a malicious
	// <img>/fetch from another site sends cross-site. (Absent header → allow,
	// for older browsers; the scan-results allowlist below is the real gate.)
	if isCrossSite(r) {
		http.Error(w, "forbidden", 403)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path required", 400)
		return
	}
	// Only open files the scanner actually found — never arbitrary paths.
	if !pathInResults(path) {
		logf("WARN", "Open-file rejected (not in scan results): %s", path)
		http.Error(w, "not a scanned file", 403)
		return
	}
	if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
		http.Error(w, "not a regular file", 404)
		return
	}
	if dangerousExts[strings.ToLower(filepath.Ext(path))] {
		logf("WARN", "Open-file rejected (executable type): %s", path)
		http.Error(w, "refusing to open executable file type", 403)
		return
	}
	logf("DEBUG", "Open file in default app: %s", path)
	switch runtime.GOOS {
	case "windows":
		// rundll32 + FileProtocolHandler opens with the default app WITHOUT
		// going through cmd.exe, avoiding shell metacharacter parsing.
		exec.Command("rundll32.exe", "url.dll,FileProtocolHandler", filepath.FromSlash(path)).Start()
	case "darwin":
		exec.Command("open", path).Start()
	default:
		exec.Command("xdg-open", path).Start()
	}
	w.WriteHeader(200)
}

func handleExport(w http.ResponseWriter, r *http.Request) {
	state.mu.RLock()
	groups := make([]*DuplicateGroup, len(state.groups))
	copy(groups, state.groups)
	state.mu.RUnlock()
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", `attachment; filename="duplicates.csv"`)
	cw := csv.NewWriter(w)
	cw.Write([]string{"Group", "Type", "Similarity %", "Role", "Path", "Name", "Bytes", "MB", "Width", "Height", "MP", "Format", "Modified", "MD5"})
	for _, g := range groups {
		typ := "perceptual"
		if g.Exact {
			typ = "exact"
		}
		sim := fmt.Sprintf("%.1f", g.Similarity*100)
		for i, img := range g.Images {
			role := "duplicate"
			if i == 0 {
				role = "keep"
			}
			cw.Write([]string{
				strconv.Itoa(g.ID), typ, sim, role,
				img.Path, img.Name,
				strconv.FormatInt(img.Size, 10),
				fmt.Sprintf("%.2f", float64(img.Size)/1024/1024),
				strconv.Itoa(img.Width), strconv.Itoa(img.Height),
				fmt.Sprintf("%.2f", img.Megapixel),
				img.Format, img.ModTime.Format(time.RFC3339), img.MD5,
			})
		}
	}
	cw.Flush()
}

func handleSmartSelect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var req SmartSelectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	state.mu.RLock()
	groups := make([]*DuplicateGroup, len(state.groups))
	copy(groups, state.groups)
	state.mu.RUnlock()
	toDel, toKeep := applyStrategy(groups, req.Strategy, req.PreferDir, 0)
	logf("INFO", "Smart-select strategy=%s: %d to delete, %d to keep", req.Strategy, len(toDel), len(toKeep))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"to_delete": toDel,
		"to_keep":   toKeep,
	})
}

// ─────────────────────────────────────────────────────────────────────
// Cache, Session, Cancel handlers
// ─────────────────────────────────────────────────────────────────────

func handleCacheStats(w http.ResponseWriter, r *http.Request) {
	entries, hits, misses, fb := cache.Stats()
	hitRate := 0.0
	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"entries":    entries,
		"hits":       hits,
		"misses":     misses,
		"hit_rate":   hitRate,
		"file_bytes": fb,
		"file_path":  cacheFile,
	})
}

func handleCacheClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	n := cache.Clear()
	// Also wipe disk thumbnail cache — both serve the same purpose
	thumbsRemoved := 0
	if entries, err := os.ReadDir(thumbCacheDir); err == nil {
		for _, e := range entries {
			os.RemoveAll(filepath.Join(thumbCacheDir, e.Name()))
			thumbsRemoved++
		}
	}
	logf("INFO", "Cache cleared: %d hash entries, %d thumbnail dirs", n, thumbsRemoved)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cleared":        n,
		"thumbs_removed": thumbsRemoved,
	})
}

func handleSessionSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var body struct {
		Selected []string `json:"selected"`
		Note     string   `json:"note"`
	}
	json.NewDecoder(r.Body).Decode(&body) // optional fields

	state.mu.RLock()
	sess := Session{
		Version:   sessionVersion,
		Timestamp: time.Now(),
		Settings:  state.settings,
		Groups:    state.groups,
		Selected:  body.Selected,
		Note:      body.Note,
	}
	state.mu.RUnlock()

	data, err := json.MarshalIndent(sess, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if err := os.WriteFile(sessionFile, data, 0644); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	logf("INFO", "Session saved: %d groups, %d selected, %d bytes",
		len(sess.Groups), len(sess.Selected), len(data))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"saved":     true,
		"timestamp": sess.Timestamp,
		"bytes":     len(data),
	})
}

func handleSessionLoad(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile(sessionFile)
	if err != nil {
		http.Error(w, "no session", 404)
		return
	}
	var sess Session
	if err := json.Unmarshal(data, &sess); err != nil {
		http.Error(w, "session corrupt: "+err.Error(), 500)
		return
	}
	if sess.Version != sessionVersion {
		http.Error(w, "session version mismatch", 400)
		return
	}

	state.mu.Lock()
	state.groups = sess.Groups
	state.settings = sess.Settings
	state.progress.Done = true
	state.progress.Groups = len(sess.Groups)
	var tw int64
	for _, g := range sess.Groups {
		tw += g.WastedSize
	}
	state.progress.WastedMB = float64(tw) / 1024 / 1024
	state.progress.Status = fmt.Sprintf("Loaded session from %s — %d groups, %.1f MB",
		sess.Timestamp.Format("Jan 2 15:04"), len(sess.Groups), state.progress.WastedMB)
	state.mu.Unlock()

	logf("INFO", "Session loaded: %d groups from %s", len(sess.Groups), sess.Timestamp)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"timestamp": sess.Timestamp,
		"groups":    sess.Groups,
		"selected":  sess.Selected,
		"settings":  sess.Settings,
		"note":      sess.Note,
	})
}

func handleSessionExists(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	data, err := os.ReadFile(sessionFile)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"exists": false})
		return
	}
	var peek struct {
		Timestamp time.Time  `json:"timestamp"`
		Groups    []struct{} `json:"groups"`
		Selected  []string   `json:"selected"`
		Note      string     `json:"note,omitempty"`
	}
	json.Unmarshal(data, &peek)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"exists":    true,
		"timestamp": peek.Timestamp,
		"groups":    len(peek.Groups),
		"selected":  len(peek.Selected),
		"note":      peek.Note,
		"bytes":     len(data),
	})
}

func handleSessionClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	err := os.Remove(sessionFile)
	if err != nil && !os.IsNotExist(err) {
		http.Error(w, err.Error(), 500)
		return
	}
	logf("INFO", "Session file cleared")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"cleared": true})
}

func handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	atomic.StoreInt64(&cancelScan, 1)
	// Kill any in-flight ffmpeg/ffprobe subprocesses so cancellation is near
	// instant instead of waiting for the slowest one to finish on its own.
	scanCancelMu.Lock()
	if scanCancelFn != nil {
		scanCancelFn()
	}
	scanCancelMu.Unlock()
	logf("INFO", "Scan cancel requested")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"cancelled": true})
}

// handleFolderPick opens a native OS folder-picker dialog and returns the
// selected absolute path. Allows multi-selection on Windows. Falls back to
// returning an empty path if the dialog is cancelled.
// handleCommonFolders returns the user's standard media folders (Pictures,
// Downloads, Desktop, Documents) that actually exist, so the UI can offer
// one-click "scan this" buttons instead of making the user type a path. Only the
// server can resolve these from the home directory, hence an endpoint.
func handleCommonFolders(w http.ResponseWriter, r *http.Request) {
	home, err := os.UserHomeDir()
	out := []map[string]string{}
	if err == nil {
		for _, c := range []struct{ label, icon, sub string }{
			{"Pictures", "🖼️", "Pictures"},
			{"Downloads", "⬇️", "Downloads"},
			{"Desktop", "🖥️", "Desktop"},
			{"Documents", "📄", "Documents"},
		} {
			p := filepath.Join(home, c.sub)
			if fi, e := os.Stat(p); e == nil && fi.IsDir() {
				out = append(out, map[string]string{"label": c.label, "icon": c.icon, "path": p})
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"folders": out})
}

func handleFolderPick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	multi := r.URL.Query().Get("multi") == "1"
	logf("DEBUG", "Folder pick requested, multi=%v", multi)

	paths := pickFolders(multi)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"paths": paths,
	})
}

// pickFolders shows the OS folder picker. Returns a list of absolute paths,
// possibly empty if the user cancelled.
func pickFolders(multi bool) []string {
	switch runtime.GOOS {
	case "windows":
		return pickFoldersWindows(multi)
	case "darwin":
		return pickFoldersMacOS(multi)
	default:
		return pickFoldersLinux(multi)
	}
}

func pickFoldersWindows(multi bool) []string {
	// Use modern WinForms FolderBrowserDialog with TopMost owner trick.
	// Without an owner the dialog can appear behind the browser window.
	// PowerShell -STA is required for COM-based dialogs.
	pickOne := `
[void][System.Reflection.Assembly]::LoadWithPartialName('System.Windows.Forms')
Add-Type -AssemblyName System.Windows.Forms
$form = New-Object System.Windows.Forms.Form
$form.TopMost = $true
$form.Opacity = 0
$form.ShowInTaskbar = $false
$form.StartPosition = 'CenterScreen'
$form.Show()
$form.Focus() | Out-Null
$d = New-Object System.Windows.Forms.FolderBrowserDialog
$d.ShowNewFolderButton = $false
$d.Description = 'Select a folder to scan'
$result = $d.ShowDialog($form)
$form.Close()
if ($result -eq [System.Windows.Forms.DialogResult]::OK) {
    Write-Output $d.SelectedPath
}
`
	out, err := exec.Command("powershell", "-NoProfile", "-STA", "-Command", pickOne).Output()
	if err != nil {
		logf("ERROR", "Folder picker (PowerShell) failed: %v", err)
		return nil
	}
	picked := strings.TrimSpace(string(out))
	if picked == "" {
		return nil
	}
	// Note: WinForms FolderBrowserDialog is single-select only.
	// True multi-select requires the Vista IFileOpenDialog with FOS_PICKFOLDERS,
	// which is awkward to invoke from PowerShell. The UI workaround is for
	// the user to click "Browse…" multiple times — we always APPEND to the
	// textarea rather than replacing.
	_ = multi
	return []string{picked}
}

func pickFoldersMacOS(multi bool) []string {
	multiArg := ""
	if multi {
		multiArg = " with multiple selections allowed"
	}
	script := fmt.Sprintf(`set chosenFolders to choose folder with prompt "Select folders to scan"%s
set output to ""
repeat with f in chosenFolders
    set output to output & POSIX path of f & linefeed
end repeat
return output`, multiArg)

	out, err := exec.Command("osascript", "-e", script).Output()
	if err != nil {
		// User cancel returns nonzero exit code; not an error
		return nil
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	results := make([]string, 0, len(lines))
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln != "" {
			results = append(results, strings.TrimRight(ln, "/"))
		}
	}
	return results
}

func pickFoldersLinux(multi bool) []string {
	// Try zenity (most common), then kdialog
	args := []string{"--file-selection", "--directory", "--title=Select folder to scan"}
	if multi {
		args = append(args, "--multiple", "--separator=\n")
	}
	if out, err := exec.Command("zenity", args...).Output(); err == nil {
		picked := strings.TrimSpace(string(out))
		if picked == "" {
			return nil
		}
		results := make([]string, 0)
		for _, ln := range strings.Split(picked, "\n") {
			if ln = strings.TrimSpace(ln); ln != "" {
				results = append(results, ln)
			}
		}
		return results
	}
	// kdialog fallback (KDE)
	if out, err := exec.Command("kdialog", "--getexistingdirectory", "--title", "Select folder").Output(); err == nil {
		picked := strings.TrimSpace(string(out))
		if picked != "" {
			return []string{picked}
		}
	}
	logf("WARN", "No folder picker tool available (tried zenity, kdialog)")
	return nil
}

// ─────────────────────────────────────────────────────────────────────
// Recycle Bin (batched on Windows)
// ─────────────────────────────────────────────────────────────────────

func batchDelete(paths []string, toTrash bool) ([]string, map[string]string) {
	deleted := make([]string, 0, len(paths))
	failed := make(map[string]string)

	if toTrash && runtime.GOOS == "windows" && len(paths) > 0 {
		var sb strings.Builder
		sb.WriteString("Add-Type -AssemblyName Microsoft.VisualBasic;")
		sb.WriteString("$paths=@(")
		for i, p := range paths {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("'")
			sb.WriteString(strings.ReplaceAll(p, "'", "''"))
			sb.WriteString("'")
		}
		sb.WriteString(");foreach($p in $paths){try{[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile($p,'OnlyErrorDialogs','SendToRecycleBin');Write-Host \"OK:$p\"}catch{Write-Host \"ERR:$p\"}}")

		out, err := exec.Command("powershell", "-NoProfile", "-Command", sb.String()).Output()
		if err != nil {
			logf("ERROR", "Batch PowerShell failed: %v", err)
		}
		okSet := make(map[string]bool)
		for _, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "OK:") {
				okSet[strings.TrimPrefix(line, "OK:")] = true
			}
		}
		for _, p := range paths {
			if okSet[p] {
				deleted = append(deleted, p)
			} else if _, err := os.Stat(p); err != nil {
				deleted = append(deleted, p)
			} else {
				failed[p] = "trash failed"
			}
		}
		return deleted, failed
	}

	for _, p := range paths {
		var err error
		if toTrash {
			err = moveToTrash(p)
		} else {
			err = os.Remove(p)
		}
		if err != nil {
			failed[p] = err.Error()
		} else {
			deleted = append(deleted, p)
		}
	}
	return deleted, failed
}

func moveToTrash(path string) error {
	switch runtime.GOOS {
	case "darwin":
		script := fmt.Sprintf(`tell application "Finder" to delete POSIX file %q`, path)
		if exec.Command("osascript", "-e", script).Run() == nil {
			return nil
		}
		return moveToDir(path, filepath.Join(os.Getenv("HOME"), ".Trash"))
	case "linux":
		if exec.Command("gio", "trash", path).Run() == nil {
			return nil
		}
		if exec.Command("trash-put", path).Run() == nil {
			return nil
		}
		home, _ := os.UserHomeDir()
		return moveToDir(path, filepath.Join(home, ".local", "share", "Trash", "files"))
	case "windows":
		ps := fmt.Sprintf(`Add-Type -AssemblyName Microsoft.VisualBasic; [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile('%s','OnlyErrorDialogs','SendToRecycleBin')`,
			strings.ReplaceAll(path, "'", "''"))
		return exec.Command("powershell", "-NoProfile", "-Command", ps).Run()
	default:
		return os.Remove(path)
	}
}

func moveToDir(src, destDir string) error {
	os.MkdirAll(destDir, 0755)
	base := filepath.Base(src)
	dest := filepath.Join(destDir, base)
	if _, err := os.Stat(dest); err == nil {
		ext := filepath.Ext(base)
		name := strings.TrimSuffix(base, ext)
		dest = filepath.Join(destDir, fmt.Sprintf("%s_%d%s", name, time.Now().UnixMilli(), ext))
	}
	if err := os.Rename(src, dest); err != nil {
		if err2 := copyFile(src, dest); err2 != nil {
			return err2
		}
		return os.Remove(src)
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	// Don't defer Close: callers delete src after copyFile succeeds, so a
	// dropped flush/close error here would mean silent data loss. Flush to
	// disk and surface the close error explicitly.
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// ─────────────────────────────────────────────────────────────────────
// Scan Engine
// ─────────────────────────────────────────────────────────────────────

// modeNeeds returns which algorithms a detection mode needs.
// Returns (doMD5, doDhash, doAhash, doPhash).
func modeNeeds(mode string) (md5, dh, ah, ph bool) {
	switch mode {
	case "exact":
		return true, false, false, false
	case "quick":
		return true, true, false, false // current default — fast & decent
	case "standard":
		return true, false, false, true // exact + DCT pHash, robust to compression
	case "robust":
		return true, true, true, false // exact + dhash + ahash
	case "smart":
		return true, true, true, true // exact + all 3 perceptual (most accurate)
	default:
		return true, true, false, false
	}
}

// currentScanCtx returns the in-flight scan's cancellation context, or a
// background context when no scan is running. Subprocess call sites that aren't
// on the explicitly ctx-threaded video/audio paths use this so they're killed on
// cancel and bounded by their own timeout.
func currentScanCtx() context.Context {
	scanCancelMu.Lock()
	defer scanCancelMu.Unlock()
	return scanCtxCur
}

func runScan(req ScanRequest) {
	// SetMemoryLimit tells Go's GC "hard cap at 70% of physical RAM".
	// When heap approaches this, GC becomes as aggressive as needed.
	// We deliberately do NOT change GOGC — lowering it (e.g. GOGC=20)
	// causes constant stop-the-world pauses that kill decode throughput.
	// SetMemoryLimit alone is sufficient and doesn't hurt CPU usage.
	// RAM budget — user-controlled (mem_budget_mb), else auto 70% of installed.
	// This is independent of the CPU worker count: it caps total memory AND
	// (below) how many images decode at once, so RAM stays bounded no matter
	// how many workers the user picks.
	budgetMB := req.MemBudgetMB
	autoBudget := budgetMB <= 0
	if autoBudget {
		budgetMB = physicalRAMBytes() * 70 / 100 / (1024 * 1024)
	}
	memLimit := budgetMB * 1024 * 1024
	if memLimit < 512*1024*1024 {
		memLimit = 512 * 1024 * 1024 // floor: 512MB
		budgetMB = 512
	}
	debug.SetMemoryLimit(memLimit)
	logf("INFO", "Memory budget: %.0f MB (%s, installed %.1f GB)",
		float64(memLimit)/1024/1024,
		map[bool]string{true: "auto 70%", false: "user-set"}[autoBudget],
		float64(physicalRAMBytes())/(1024*1024*1024))

	tickerDone := make(chan struct{})
	defer func() {
		close(tickerDone) // stop progress ticker goroutine unconditionally
		state.mu.Lock()
		state.scanning = false
		state.mu.Unlock()
		// Keep a moderate post-scan memory limit so GC stays aggressive about
		// returning pages to the OS. Without this, the runtime holds freed heap
		// pages indefinitely (RAM stays high in Task Manager).
		postLimit := physicalRAMBytes() * 30 / 100
		if postLimit < 256*1024*1024 {
			postLimit = 256 * 1024 * 1024
		}
		debug.SetMemoryLimit(postLimit)
		runtime.GC()
		debug.FreeOSMemory()
		logf("INFO", "Post-scan cleanup: memory limit set to %.0f MB, GC+FreeOSMemory done",
			float64(postLimit)/1024/1024)
	}()

	atomic.StoreInt64(&cancelScan, 0)

	// Per-scan context: cancelled when runScan returns (defer) or when the user
	// hits cancel (handleCancel calls scanCancelFn). exec.CommandContext ties
	// ffmpeg/ffprobe lifetimes to this, so cancel kills them immediately and a
	// per-call timeout (in video.go) bounds any single hung subprocess.
	scanCtx, scanCtxCancel := context.WithCancel(context.Background())
	scanCancelMu.Lock()
	scanCancelFn = scanCtxCancel
	scanCtxCur = scanCtx
	scanCancelMu.Unlock()
	defer func() {
		scanCtxCancel()
		scanCancelMu.Lock()
		scanCancelFn = nil
		scanCtxCur = context.Background()
		scanCancelMu.Unlock()
	}()

	setStatus("Walking directories...")
	if req.Category == "" {
		req.Category = "images"
	}
	logf("INFO", "Scan started — category=%s mode=%s threshold=%d useCache=%v",
		req.Category, req.Mode, req.Threshold, req.UseCache)
	startTime := time.Now()

	state.mu.Lock()
	state.settings = req
	state.mu.Unlock()

	doMD5, doDhash, doAhash, doPhash := modeNeeds(req.Mode)

	// Video "visual" mode = frame-based perceptual matching. Falls back to
	// exact (partial-hash) when ffmpeg/ffprobe are unavailable.
	videoVisual := req.Category == "videos" && req.Mode == "visual"
	if videoVisual && !videoToolsAvailable() {
		logf("WARN", "ffmpeg/ffprobe not found — falling back to exact matching for videos")
		setStatus("ffmpeg not found — using exact (partial-hash) matching")
		videoVisual = false
		req.Mode = "exact"
	}

	// Audio "acoustic" mode = Chromaprint fingerprint matching. Falls back to
	// exact (partial-hash) when fpcalc is unavailable.
	audioAcoustic := req.Category == "audio" && req.Mode == "acoustic"
	if audioAcoustic && !audioToolsAvailable() {
		logf("WARN", "fpcalc not found — falling back to exact matching for audio")
		setStatus("fpcalc not found — using exact (partial-hash) matching")
		audioAcoustic = false
		req.Mode = "exact"
	}

	if videoVisual || audioAcoustic {
		doMD5, doDhash, doAhash, doPhash = false, false, false, false
	} else if !isImageCategory(req.Category) {
		// Non-image categories cannot be perceptually hashed — force MD5-only
		doDhash, doAhash, doPhash = false, false, false
		doMD5 = true
	}
	doDecode := doDhash || doAhash || doPhash
	logf("INFO", "Algorithms: md5=%v dhash=%v ahash=%v phash=%v", doMD5, doDhash, doAhash, doPhash)

	// Pick extension set based on category, unless user provided explicit filter
	var exts map[string]bool
	if len(req.ExtsFilter) > 0 {
		exts = make(map[string]bool)
		for _, e := range req.ExtsFilter {
			e = strings.ToLower(e)
			if !strings.HasPrefix(e, ".") {
				e = "." + e
			}
			exts[e] = true
		}
	} else {
		exts = extsForCategory(req.Category)
	}

	files := collectFiles(req.Dirs, req.SkipHidden, req.MinSizeKB, exts, req.ExcludeDirs)
	total := int64(len(files))
	atomic.StoreInt64(&procCount, 0)
	logf("INFO", "Found %d files matching criteria", total)

	state.mu.Lock()
	state.progress.Total = total
	state.progress.Status = fmt.Sprintf("Found %d files, processing...", total)
	state.mu.Unlock()

	if total == 0 {
		state.mu.Lock()
		state.progress.Status = "No matching files found in the specified directories"
		state.progress.Done = true
		state.mu.Unlock()
		return
	}

	// Speed: pre-group by size; unique-size files skip MD5 (only when doMD5 is on)
	bySize := make(map[int64][]*FileEntry, len(files))
	for _, f := range files {
		bySize[f.Size] = append(bySize[f.Size], f)
	}
	needMD5Map := make(map[string]bool, len(files))
	uniqueSize := 0
	for sz, group := range bySize {
		if sz > 0 && len(group) >= 2 {
			for _, f := range group {
				needMD5Map[f.Path] = true
			}
		} else {
			uniqueSize += len(group)
		}
	}
	if doMD5 {
		logf("INFO", "Size pre-grouping: %d files have unique size (skipping MD5), %d need MD5",
			uniqueSize, len(needMD5Map))
	}

	// ── Worker count ─────────────────────────────────────────────────
	// workerCount = how many images are decoded simultaneously.
	// This is the ONLY number that matters:
	//   • It directly equals the CPU cores actively used for hashing.
	//   • RAM peak ≈ workerCount × 96MB (NRGBA buffer for a 24MP image).
	//
	// Why one number controls both:
	//   A goroutine decodes ONE image, hashes it (CPU), then moves on.
	//   Peak RAM = max simultaneous decoders × bytes-per-image.
	//   Peak CPU = goroutines actively decoding / total cores.
	//
	// With 22 cores and 16GB RAM:
	//   workerCount=22 → 22 cores busy → 22×48MB = 1.0GB (6% of 16GB) ✓
	//   workerCount=4  → 4 cores busy  → 4×48MB  = 192MB (1% of 16GB) ✓

	// Peak per concurrent decode is just the decoded buffer now — the hashes read
	// luma straight from it (no 96MB NRGBA copy). A 24MP JPEG decodes to ~36MB of
	// YCbCr; 48MB leaves headroom for RGBA-decoding formats. Halving the old
	// estimate lets ~2× more decodes run under a tight RAM budget.
	const perImageMB int64 = 48
	numCPU := runtime.NumCPU()
	ram := physicalRAMBytes()

	workerCount := req.Threads
	if workerCount <= 0 {
		// Auto: use all CPU cores, verify RAM is sufficient.
		// For 16GB: 22 × 96MB = 2.1GB = 13% — always fine.
		// Cap only if RAM is genuinely tiny (e.g. 1GB system).
		workerCount = numCPU
		maxByRAM := int(ram * 70 / 100 / (perImageMB * 1024 * 1024))
		if maxByRAM < 1 {
			maxByRAM = 1
		}
		if workerCount > maxByRAM {
			workerCount = maxByRAM
		}
	}
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > numCPU*2 {
		workerCount = numCPU * 2 // allow slight oversubscription if user wants
	}

	logf("INFO", "Workers: %d (CPU cores: %d, RAM: %.1f GB, est. peak: ~%d MB)",
		workerCount, numCPU, float64(ram)/(1024*1024*1024),
		int64(workerCount)*perImageMB)

	// Decode concurrency is gated by the RAM budget so peak memory stays bounded
	// no matter how many CPU workers run. Each decode needs ~perImageMB.
	decodeConc := int(budgetMB / perImageMB)
	if decodeConc < 1 {
		decodeConc = 1
	}
	if decodeConc > workerCount {
		decodeConc = workerCount // never more decode slots than workers
	}
	decodeSem := make(chan struct{}, decodeConc)
	logf("INFO", "Decode concurrency (RAM-gated): %d (budget %d MB ÷ %d MB/image)",
		decodeConc, budgetMB, perImageMB)

	// ── Subprocess worker count (video/audio) ────────────────────────
	// Video/audio fingerprinting shells out to ffmpeg/ffprobe/fpcalc, and EACH
	// of those spawns its OWN decode threads. Reusing the image workerCount
	// (== numCPU) here launches numCPU processes × numCPU threads — quadratic
	// oversubscription that pegs every core and freezes the laptop. Instead run
	// a small pool of processes, each capped to videoThreads threads, so the
	// total stays near ~75% of cores and the machine stays responsive.
	videoThreads := 2
	if numCPU <= 4 {
		videoThreads = 1
	}
	subprocWorkers := (numCPU * 3 / 4) / videoThreads
	if subprocWorkers < 1 {
		subprocWorkers = 1
	}
	if subprocWorkers > 8 {
		subprocWorkers = 8
	}
	// A user who lowered the worker slider wants an even quieter machine — honor
	// it as a ceiling (but never raise the subprocess pool above the auto value).
	if req.Threads > 0 && req.Threads < subprocWorkers {
		subprocWorkers = req.Threads
	}
	setVideoThreadLimit(videoThreads)
	logf("INFO", "Subprocess workers (video/audio): %d × %d threads each (~%d%% CPU)",
		subprocWorkers, videoThreads, subprocWorkers*videoThreads*100/numCPU)

	// Progress ticker. Started before the video branch so both the video and
	// image pipelines get live progress (Processed/Rate/Heap) from procCount.
	// ReadMemStats is stop-the-world — run it every 5s on a separate ticker.
	// Use atomic uint64 (bits of float64) to pass heapMB safely between goroutines.
	// tickerDone is closed by the deferred cleanup, so this goroutine always exits.
	var heapMBbits uint64 // stores math.Float64bits(heapMB) atomically
	go func() {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		atomic.StoreUint64(&heapMBbits, math.Float64bits(float64(ms.HeapInuse)/1024/1024))
		memTick := time.NewTicker(5 * time.Second)
		progTick := time.NewTicker(200 * time.Millisecond)
		defer memTick.Stop()
		defer progTick.Stop()
		for {
			select {
			case <-tickerDone:
				return // scan finished or cancelled — stop goroutine
			case <-memTick.C:
				runtime.ReadMemStats(&ms)
				atomic.StoreUint64(&heapMBbits, math.Float64bits(float64(ms.HeapInuse)/1024/1024))
			case <-progTick.C:
				done := atomic.LoadInt64(&procCount)
				if done > total {
					done = total
				}
				elapsed := time.Since(startTime).Seconds()
				rate := 0.0
				if elapsed > 0 {
					rate = math.Round(float64(done) / elapsed)
				}
				heap := math.Float64frombits(atomic.LoadUint64(&heapMBbits))
				state.mu.Lock()
				state.progress.Processed = done
				state.progress.Rate = rate
				state.progress.HeapMB = heap
				state.mu.Unlock()
			}
		}
	}()

	// ── Video visual mode: probe + fingerprint + frame-based grouping ──
	if videoVisual {
		images := produceVideoInfos(scanCtx, files, req, subprocWorkers)
		files = nil
		bySize = nil
		needMD5Map = nil
		cancelled := atomic.LoadInt64(&cancelScan) != 0

		if req.UseCache {
			setStatus("Saving cache...")
			if err := cache.Save(cacheFile); err != nil {
				logf("WARN", "Cache save failed: %v", err)
			}
		}

		// Group whatever was fingerprinted. On cancel this surfaces the partial
		// results found so far instead of throwing the completed work away.
		setStatus("Analysing duplicates...")
		groups := groupVideoDuplicates(images, req.Threshold)
		images = nil

		var tw int64
		for _, g := range groups {
			tw += g.WastedSize
		}
		suf := "s"
		if len(groups) == 1 {
			suf = ""
		}
		elapsed := time.Since(startTime).Seconds()
		state.mu.Lock()
		state.groups = groups
		state.progress.Processed = total
		state.progress.Groups = len(groups)
		state.progress.WastedMB = float64(tw) / 1024 / 1024
		if cancelled {
			state.progress.Status = fmt.Sprintf("Cancelled after %.1fs — %d partial group%s found (%.1f MB reclaimable)",
				elapsed, len(groups), suf, float64(tw)/1024/1024)
		} else {
			state.progress.Status = fmt.Sprintf("Done in %.1fs! Found %d duplicate group%s — %.1f MB reclaimable",
				elapsed, len(groups), suf, float64(tw)/1024/1024)
		}
		state.progress.Done = true
		state.mu.Unlock()
		logf("INFO", "Video scan %s in %.1fs: %d groups, %.1f MB reclaimable",
			map[bool]string{true: "cancelled", false: "complete"}[cancelled],
			elapsed, len(groups), float64(tw)/1024/1024)
		return
	}

	// ── Audio acoustic mode: fpcalc fingerprint + chroma-based grouping ──
	if audioAcoustic {
		simThreshold := float64(req.Threshold) / 100.0
		if simThreshold <= 0 || simThreshold > 1 {
			simThreshold = 0.80 // default Balanced
		}
		images := produceAudioInfos(scanCtx, files, req, subprocWorkers)
		files = nil
		bySize = nil
		needMD5Map = nil
		cancelled := atomic.LoadInt64(&cancelScan) != 0

		if req.UseCache {
			setStatus("Saving cache...")
			if err := cache.Save(cacheFile); err != nil {
				logf("WARN", "Cache save failed: %v", err)
			}
		}

		// Group whatever was fingerprinted. On cancel this surfaces the partial
		// results found so far instead of throwing the completed work away.
		setStatus("Analysing duplicates...")
		groups := groupAudioDuplicates(images, simThreshold)
		images = nil

		var tw int64
		for _, g := range groups {
			tw += g.WastedSize
		}
		suf := "s"
		if len(groups) == 1 {
			suf = ""
		}
		elapsed := time.Since(startTime).Seconds()
		state.mu.Lock()
		state.groups = groups
		state.progress.Processed = total
		state.progress.Groups = len(groups)
		state.progress.WastedMB = float64(tw) / 1024 / 1024
		if cancelled {
			state.progress.Status = fmt.Sprintf("Cancelled after %.1fs — %d partial group%s found (%.1f MB reclaimable)",
				elapsed, len(groups), suf, float64(tw)/1024/1024)
		} else {
			state.progress.Status = fmt.Sprintf("Done in %.1fs! Found %d duplicate group%s — %.1f MB reclaimable",
				elapsed, len(groups), suf, float64(tw)/1024/1024)
		}
		state.progress.Done = true
		state.mu.Unlock()
		logf("INFO", "Audio scan %s in %.1fs: %d groups, %.1f MB reclaimable",
			map[bool]string{true: "cancelled", false: "complete"}[cancelled],
			elapsed, len(groups), float64(tw)/1024/1024)
		return
	}

	// Two-stage pipeline:
	//   Stage 1 — I/O goroutines: read file bytes, compute partial MD5.
	//             Disk-bound; run independently so disk stays saturated.
	//   Stage 2 — CPU goroutines (workerCount): decode + hash.
	//             Each holds one decoded image; workerCount bounds peak RAM.

	ioCount := numCPU / 2
	if ioCount < 2 {
		ioCount = 2
	}
	if ioCount > 16 {
		ioCount = 16
	}

	type md5Job struct {
		f   *FileEntry
		md5 string
	}

	// Buffer sizes: large enough that neither stage starves waiting on channels
	fileJobs := make(chan *FileEntry, ioCount*8)
	md5Jobs := make(chan md5Job, workerCount*4)
	results := make(chan *ImageInfo, workerCount*4)

	var ioWg sync.WaitGroup
	var cpuWg sync.WaitGroup

	// Stage 1 — I/O workers
	for i := 0; i < ioCount; i++ {
		ioWg.Add(1)
		go func() {
			defer ioWg.Done()
			for f := range fileJobs {
				if atomic.LoadInt64(&cancelScan) != 0 {
					atomic.AddInt64(&procCount, 1)
					continue
				}
				md5hex := ""
				if doMD5 && needMD5Map[f.Path] {
					if req.UseCache {
						if ce, ok := cache.Get(f.Path, f.Size, f.ModTime); ok && ce.MD5 != "" {
							md5hex = ce.MD5
						}
					}
					if md5hex == "" {
						if h, err := computeContentHash(f.Path, f.Size); err == nil {
							md5hex = h
						}
					}
				}
				md5Jobs <- md5Job{f, md5hex}
			}
		}()
	}

	// Stage 2 — CPU/decode workers
	for i := 0; i < workerCount; i++ {
		cpuWg.Add(1)
		go func() {
			defer cpuWg.Done()
			for job := range md5Jobs {
				if atomic.LoadInt64(&cancelScan) != 0 {
					atomic.AddInt64(&procCount, 1)
					continue
				}
				f := job.f
				ext := strings.ToLower(filepath.Ext(f.Path))
				format := strings.TrimPrefix(ext, ".")
				if format == "jpeg" {
					format = "jpg"
				}
				info := &ImageInfo{
					Path:    f.Path,
					Name:    filepath.Base(f.Path),
					Dir:     filepath.Dir(f.Path),
					Size:    f.Size,
					Format:  format,
					ModTime: f.ModTime,
					MD5:     job.md5,
				}

				// Populate from cache if available
				needDecode := doDecode && isImageCategory(req.Category)
				if req.UseCache {
					if ce, ok := cache.Get(f.Path, f.Size, f.ModTime); ok {
						if ce.Width > 0 {
							info.Width = ce.Width
							info.Height = ce.Height
							info.Megapixel = math.Round(float64(ce.Width)*float64(ce.Height)/100000) / 10
						}
						if ce.Format != "" {
							info.Format = ce.Format
						}
						if info.MD5 == "" {
							info.MD5 = ce.MD5
						}
						if ce.HasDhash {
							info.DHash = ce.DHash
						}
						if ce.HasAhash {
							info.AHash = ce.AHash
						}
						if ce.HasPhash {
							info.PHash = ce.PHash
						}
						if ce.Decoded {
							info.Decoded = true
							needDecode = false // all hashes already in cache
						}
						info.Quick = ce.Quick // carry content identity (no re-read on a hit)
					}
				}

				// Decode → 3× hash sampled straight from the decoded image (luma read
				// from the JPEG Y plane — no full-resolution RGBA buffer). decodeSem
				// caps how many decode at once = RAM budget.
				decodedNow := false
				var thumbBytes []byte
				if needDecode {
					decodeSem <- struct{}{}
					if img, fmt2, err := decodeImageFile(f.Path); err == nil {
						b := img.Bounds()
						info.Width, info.Height = b.Dx(), b.Dy()
						info.Megapixel = math.Round(float64(info.Width)*float64(info.Height)/100000) / 10
						if fmt2 != "" {
							info.Format = fmt2
						}
						info.DHash = dHashFast(img)
						info.AHash = aHashFast(img)
						info.PHash = pHashFast(img)
						// Piggyback the thumbnail off this decode. We already paid the
						// costly decode (HEIC/large JPEG); the downscale is near-free, so
						// viewing results no longer triggers a second full decode per file.
						if tb, terr := encodeThumbDirect(img); terr == nil {
							thumbBytes = tb
						}
						img = nil // free decoded buffer NOW, before next decode
						info.Decoded = true
						decodedNow = true
					}
					<-decodeSem
					// Persist the thumbnail outside the decode semaphore — disk I/O, not
					// RAM-bound. Skip if one already exists (idempotent across re-scans).
					if thumbBytes != nil {
						dst := thumbCachePath(f.Path, f.Size, f.ModTime.UnixNano())
						if _, e := os.Stat(dst); e != nil {
							if werr := writeThumbAtomic(dst, thumbBytes); werr != nil {
								logf("DEBUG", "Scan thumbnail write failed: %s: %v", filepath.Base(f.Path), werr)
							}
						}
					}
				}

				// Content identity for the rename-resilient cache. Reuse the MD5 we
				// already hashed when present (free); otherwise hash now ONLY if we
				// just read the file to decode it — never on a pure cache hit.
				if info.Quick == "" {
					if info.MD5 != "" {
						info.Quick = info.MD5
					} else if decodedNow {
						info.Quick = contentID(f.Path, f.Size)
					}
				}

				// Write back to cache
				if req.UseCache {
					cache.Set(f.Path, &CacheEntry{
						Size: f.Size, ModUnix: f.ModTime.UnixNano(),
						Width: info.Width, Height: info.Height, Format: info.Format,
						MD5: info.MD5, DHash: info.DHash, AHash: info.AHash, PHash: info.PHash,
						Quick:    info.Quick,
						Decoded:  info.Decoded,
						HasDhash: info.Decoded,
						HasAhash: info.Decoded,
						HasPhash: info.Decoded,
					})
				}

				results <- info
				atomic.AddInt64(&procCount, 1)
			}
		}()
	}

	// Feed files → Stage 1, chain shutdown through stages
	go func() {
		for _, f := range files {
			if atomic.LoadInt64(&cancelScan) != 0 {
				break
			}
			fileJobs <- f
		}
		close(fileJobs)
		ioWg.Wait()
		close(md5Jobs)
		cpuWg.Wait()
		close(results)
	}()

	images := make([]*ImageInfo, 0, total)
	for img := range results {
		images = append(images, img)
	}

	// Release large intermediate structures that are no longer needed.
	// These held references to every FileEntry and prevented GC during
	// the result-collection phase above.
	files = nil
	bySize = nil
	needMD5Map = nil

	if atomic.LoadInt64(&cancelScan) != 0 {
		// Save partial cache even on cancel — useful work was done
		if req.UseCache {
			setStatus("Saving cache...")
			if err := cache.Save(cacheFile); err != nil {
				logf("WARN", "Cache save failed: %v", err)
			}
		}
		images = nil // release non-duplicate image objects
		// Deferred cleanup handles GC + FreeOSMemory
		state.mu.Lock()
		state.progress.Status = "Scan cancelled"
		state.progress.Done = true
		state.mu.Unlock()
		logf("INFO", "Scan cancelled after %.1fs", time.Since(startTime).Seconds())
		return
	}

	logf("INFO", "Decoding stats: native=%d external=%d failed=%d",
		atomic.LoadInt64(&decNative),
		atomic.LoadInt64(&decExt),
		atomic.LoadInt64(&decFail))

	if req.UseCache {
		_, hits, misses, _ := cache.Stats()
		logf("INFO", "Cache stats: hits=%d misses=%d", hits, misses)
		setStatus("Saving cache...")
		t0 := time.Now()
		if err := cache.Save(cacheFile); err != nil {
			logf("WARN", "Cache save failed: %v", err)
		} else {
			ent, _, _, fb := cache.Stats()
			logf("INFO", "Cache saved in %v: %d entries, %d bytes", time.Since(t0), ent, fb)
		}
	}

	setStatus("Analysing duplicates...")
	groups := findDuplicates(images, req.Threshold, doMD5, doDhash, doAhash, doPhash, req.Mode)

	// Release the full images slice — only groups (duplicate pairs) need to
	// stay in memory. Non-duplicate ImageInfo objects can now be GC'd.
	images = nil

	var tw int64
	for _, g := range groups {
		tw += g.WastedSize
	}

	state.mu.Lock()
	state.groups = groups
	state.progress.Processed = total
	state.progress.Groups = len(groups)
	state.progress.WastedMB = float64(tw) / 1024 / 1024
	suf := "s"
	if len(groups) == 1 {
		suf = ""
	}
	elapsed := time.Since(startTime).Seconds()
	state.progress.Status = fmt.Sprintf("Done in %.1fs! Found %d duplicate group%s — %.1f MB reclaimable",
		elapsed, len(groups), suf, float64(tw)/1024/1024)
	state.progress.Done = true
	state.mu.Unlock()

	logf("INFO", "Scan complete in %.1fs: %d groups, %.1f MB reclaimable",
		elapsed, len(groups), float64(tw)/1024/1024)

	// The deferred cleanup function handles GC + FreeOSMemory + memory limit
	// reset. This ensures memory is returned to the OS even on early returns.
}

func setStatus(s string) {
	state.mu.Lock()
	state.progress.Status = s
	state.mu.Unlock()
}

// ─────────────────────────────────────────────────────────────────────
// File Walking
// ─────────────────────────────────────────────────────────────────────

type FileEntry struct {
	Path    string
	Size    int64
	ModTime time.Time
}

func collectFiles(dirs []string, skipHidden bool, minSizeKB int64, exts map[string]bool, excludeDirs []string) []*FileEntry {
	var files []*FileEntry
	seen := make(map[string]bool)

	// Normalize exclude paths
	excludes := make([]string, 0, len(excludeDirs))
	for _, e := range excludeDirs {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if abs, err := filepath.Abs(e); err == nil {
			excludes = append(excludes, abs)
		} else {
			excludes = append(excludes, e)
		}
	}

	isExcluded := func(path string) bool {
		for _, e := range excludes {
			if strings.HasPrefix(path, e) {
				return true
			}
		}
		return false
	}

	for _, dir := range dirs {
		if atomic.LoadInt64(&cancelScan) != 0 {
			break // user pressed Stop before/while enumerating folders
		}
		// WalkDir avoids an os.Lstat per entry (it uses the readdir-provided
		// type), so we only stat (via d.Info) files that pass the cheap ext/hidden
		// filters. This noticeably cuts traversal cost on large/NAS trees.
		filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			// Abort the whole walk promptly when the user clicks Stop — large or
			// slow (NAS) trees could otherwise keep enumerating after cancel.
			if atomic.LoadInt64(&cancelScan) != 0 {
				return filepath.SkipAll
			}
			if err != nil {
				// Skip unreadable entries (e.g. a NAS subdir that briefly drops
				// or denies access) but log so the user knows why files are missing.
				logf("WARN", "Walk error (skipping): %s: %v", path, err)
				return nil
			}
			if d.IsDir() {
				if skipHidden && strings.HasPrefix(d.Name(), ".") && path != dir {
					return filepath.SkipDir
				}
				if isExcluded(path) {
					return filepath.SkipDir
				}
				return nil
			}
			if skipHidden && strings.HasPrefix(d.Name(), ".") {
				return nil
			}
			if !exts[strings.ToLower(filepath.Ext(d.Name()))] {
				return nil
			}
			info, ierr := d.Info()
			if ierr != nil {
				logf("WARN", "Stat error (skipping): %s: %v", path, ierr)
				return nil
			}
			if minSizeKB > 0 && info.Size() < minSizeKB*1024 {
				return nil
			}
			abs, err2 := filepath.Abs(path)
			if err2 != nil {
				abs = path
			}
			if isExcluded(abs) {
				return nil
			}
			if !seen[abs] {
				seen[abs] = true
				files = append(files, &FileEntry{
					Path:    abs,
					Size:    info.Size(),
					ModTime: info.ModTime(),
				})
			}
			return nil
		})
	}
	return files
}

// ─────────────────────────────────────────────────────────────────────
// Image Processing
// ─────────────────────────────────────────────────────────────────────

func decodeImageFile(path string) (image.Image, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	img, format, err := image.Decode(f)
	f.Close()
	if err == nil {
		atomic.AddInt64(&decNative, 1)
		if format == "jpeg" {
			format = "jpg"
		}
		return img, format, nil
	}

	ext := strings.ToLower(filepath.Ext(path))
	format = strings.TrimPrefix(ext, ".")

	// Use os.CreateTemp so the OS guarantees a unique name even with
	// many concurrent decodes (avoids race on platforms where time.Now's
	// resolution is coarse, e.g. older Windows ~100ns).
	tmpFile, err := os.CreateTemp("", "dupcleaner_*.jpg")
	if err != nil {
		atomic.AddInt64(&decFail, 1)
		return nil, format, fmt.Errorf("create temp: %w", err)
	}
	tmp := tmpFile.Name()
	tmpFile.Close() // external tools will write to it
	defer os.Remove(tmp)

	// Bound each external decoder so a corrupt file can't hang a scan worker, and
	// tie it to the scan context so cancel kills it immediately.
	const externalDecodeTimeout = 60 * time.Second
	tryCmd := func(name string, args ...string) bool {
		cctx, cancel := context.WithTimeout(currentScanCtx(), externalDecodeTimeout)
		defer cancel()
		return exec.CommandContext(cctx, name, args...).Run() == nil
	}

	converted := false
	if runtime.GOOS == "darwin" {
		converted = tryCmd("sips", "-s", "format", "jpeg", "-s", "formatOptions", "80", path, "--out", tmp)
	}
	if !converted {
		converted = tryCmd("magick", path+"[0]", "-quality", "80", tmp)
	}
	if !converted {
		converted = tryCmd("convert", path+"[0]", "-quality", "80", tmp)
	}
	if !converted {
		converted = tryCmd("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", path, "-frames:v", "1", tmp, "-y")
	}
	if !converted {
		atomic.AddInt64(&decFail, 1)
		return nil, format, fmt.Errorf("no decoder for %s", ext)
	}
	f2, err := os.Open(tmp)
	if err != nil {
		return nil, format, err
	}
	defer f2.Close()
	img, _, err = image.Decode(f2)
	if err == nil {
		atomic.AddInt64(&decExt, 1)
	}
	return img, format, err
}

// ─────────────────────────────────────────────────────────────────────
// Duplicate Finding
// ─────────────────────────────────────────────────────────────────────

// findDuplicates finds groups across selected algorithms.
// Phase 1: exact MD5 matches.
// Phase 2: perceptual matches via union-find based on the active algorithms.
// In smart mode (multiple perceptual algos), images are grouped if 2-of-3 hashes
// are within threshold.
func findDuplicates(images []*ImageInfo, maxDist int, doMD5, doDhash, doAhash, doPhash bool, mode string) []*DuplicateGroup {
	if len(images) == 0 {
		return nil
	}
	var groups []*DuplicateGroup
	gid := 0
	inExact := make(map[string]bool)

	// Phase 1: exact matches via MD5
	if doMD5 {
		byMD5 := make(map[string][]*ImageInfo)
		for _, img := range images {
			if img.MD5 == "" {
				continue
			}
			byMD5[img.MD5] = append(byMD5[img.MD5], img)
		}
		for _, imgs := range byMD5 {
			if len(imgs) < 2 {
				continue
			}
			sort.Slice(imgs, func(i, j int) bool { return imgs[i].ModTime.Before(imgs[j].ModTime) })
			var total int64
			for _, img := range imgs {
				total += img.Size
			}
			wasted := total - largestSize(imgs)
			if wasted < 0 {
				wasted = 0
			}
			groups = append(groups, &DuplicateGroup{
				ID: gid, Images: imgs, Exact: true, Algorithm: "md5", Similarity: 1.0,
				TotalSize: total, WastedSize: wasted,
			})
			gid++
			for _, img := range imgs {
				inExact[img.Path] = true
			}
		}
	}

	// Phase 2: perceptual matches via VP-tree (O(N log N) instead of O(N²))
	doPerceptual := doDhash || doAhash || doPhash
	if doPerceptual && maxDist > 0 {
		rem := make([]*ImageInfo, 0, len(images)/2)
		for _, img := range images {
			if !inExact[img.Path] && img.Decoded {
				rem = append(rem, img)
			}
		}

		algo := "perceptual"
		switch mode {
		case "quick":
			algo = "dhash"
		case "standard":
			algo = "phash"
		case "robust":
			algo = "ahash+dhash"
		case "smart":
			algo = "smart"
		}

		// VP-tree grouping: O(N log N) instead of O(N²)
		rawGroups := findPerceptualGroupsVP(rem, maxDist, doDhash, doAhash, doPhash, mode)

		for _, grp := range rawGroups {
			// Compute average similarity across all active hash types
			var ds, dn int
			for a := 0; a < len(grp); a++ {
				for b := a + 1; b < len(grp); b++ {
					if doDhash {
						ds += hamming(grp[a].DHash, grp[b].DHash)
						dn++
					}
					if doAhash {
						ds += hamming(grp[a].AHash, grp[b].AHash)
						dn++
					}
					if doPhash {
						ds += hamming(grp[a].PHash, grp[b].PHash)
						dn++
					}
				}
			}
			sim := 1.0
			if dn > 0 {
				sim = 1.0 - float64(ds)/float64(dn)/64.0
			}
			sort.Slice(grp, func(a, b int) bool { return grp[a].ModTime.Before(grp[b].ModTime) })
			var total int64
			for _, img := range grp {
				total += img.Size
			}
			wasted := total - largestSize(grp)
			if wasted < 0 {
				wasted = 0
			}
			groups = append(groups, &DuplicateGroup{
				ID: gid, Images: grp, Exact: false, Algorithm: algo, Similarity: sim,
				TotalSize: total, WastedSize: wasted,
			})
			gid++
		}
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].WastedSize > groups[j].WastedSize })
	return groups
}

// ─────────────────────────────────────────────────────────────────────
// Precomputed cosine table for 32-point DCT-II (used by pHashFast)
// dctCos[k][n] = cos((2n+1)*k*π / 64)
// ─────────────────────────────────────────────────────────────────────
var dctCos [32][32]float64

func init() {
	for k := 0; k < 32; k++ {
		for n := 0; n < 32; n++ {
			dctCos[k][n] = math.Cos(float64(2*n+1) * float64(k) * math.Pi / 64.0)
		}
	}
}

// dct1d performs a 32-point DCT-II on input.
func dct1d(input *[32]float64, output *[32]float64) {
	for k := 0; k < 32; k++ {
		var s float64
		for n := 0; n < 32; n++ {
			s += input[n] * dctCos[k][n]
		}
		output[k] = s
	}
}

func hamming(a, b uint64) int { return bits.OnesCount64(a ^ b) }

func largestSize(imgs []*ImageInfo) int64 {
	if len(imgs) == 0 {
		return 0
	}
	m := imgs[0].Size
	for _, img := range imgs[1:] {
		if img.Size > m {
			m = img.Size
		}
	}
	return m
}

// physicalRAMBytes returns total physical RAM in bytes.
// Detected once via OS-specific commands and cached.
var (
	cachedRAM     int64
	cachedRAMOnce sync.Once
)

func physicalRAMBytes() int64 {
	cachedRAMOnce.Do(func() {
		cachedRAM = detectRAM()
	})
	return cachedRAM
}

// windowsInstalledRAM returns total installed physical RAM in bytes by summing
// the SMBIOS memory-module capacities — the amount printed on the spec sheet
// (e.g. 16 GB), not the slightly-lower OS-visible figure. Returns 0 on failure
// so detectRAM can fall back.
func windowsInstalledRAM() int64 {
	// wmic is fast (no .NET startup) and still present on current Windows builds.
	if out, err := exec.Command("wmic", "MEMORYCHIP", "get", "Capacity", "/Value").Output(); err == nil {
		var sum int64
		for _, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "Capacity=") {
				if v, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "Capacity=")), 10, 64); err == nil {
					sum += v
				}
			}
		}
		if sum > 0 {
			return sum
		}
	}
	// Forward-compatible fallback (wmic is being phased out of Windows): CIM.
	if out, err := exec.Command("powershell", "-NoProfile", "-Command",
		"(Get-CimInstance Win32_PhysicalMemory | Measure-Object -Property Capacity -Sum).Sum").Output(); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); err == nil && v > 0 {
			return v
		}
	}
	return 0
}

func detectRAM() int64 {
	switch runtime.GOOS {
	case "windows":
		// Prefer INSTALLED physical RAM (summed from the SMBIOS memory modules) so
		// the UI matches the number on the machine's spec sheet (e.g. 16 GB). The
		// OS "visible"/TotalPhysicalMemory figures read a few hundred MB lower
		// (~15.5 GB on a 16 GB machine) because firmware/iGPU reserve some — which
		// users read as the app reporting the "wrong" amount.
		if v := windowsInstalledRAM(); v > 0 {
			return v
		}
		// wmic is much faster than PowerShell (no .NET startup overhead)
		out, err := exec.Command("wmic", "OS", "get", "TotalVisibleMemorySize", "/Value").Output()
		if err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "TotalVisibleMemorySize=") {
					s := strings.TrimPrefix(line, "TotalVisibleMemorySize=")
					if v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil && v > 0 {
						return v * 1024 // KB → bytes
					}
				}
			}
		}
		// Fallback: PowerShell (slower but more reliable on some configs)
		out2, err2 := exec.Command("powershell", "-NoProfile", "-Command",
			"(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory").Output()
		if err2 == nil {
			if v, err3 := strconv.ParseInt(strings.TrimSpace(string(out2)), 10, 64); err3 == nil && v > 0 {
				return v
			}
		}
	case "darwin":
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err == nil {
			if v, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); err == nil && v > 0 {
				return v
			}
		}
	case "linux":
		data, err := os.ReadFile("/proc/meminfo")
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "MemTotal:") {
					fields := strings.Fields(line)
					if len(fields) >= 2 {
						if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
							return v * 1024
						}
					}
				}
			}
		}
	}
	logf("WARN", "RAM detection failed — using 8GB fallback")
	return 8 * 1024 * 1024 * 1024
}

// ─────────────────────────────────────────────────────────────────────
// Fast pixel access, perceptual hashing, resize, VP-tree
// ─────────────────────────────────────────────────────────────────────

// toNRGBA converts any image.Image to a *image.NRGBA whose .Pix slice
// gives direct byte access as [R, G, B, A, R, G, B, A, ...].
//
// If src is already *image.NRGBA the same pointer is returned (zero copy).
// All other types use draw.Draw for a single conversion pass.
func toNRGBA(src image.Image) *image.NRGBA {
	// Fast path — no allocation at all
	if n, ok := src.(*image.NRGBA); ok {
		return n
	}
	// Note: RGBA uses premultiplied alpha and NRGBA uses straight alpha.
	// They are NOT layout-compatible for non-opaque images, so we must
	// always draw.Draw for RGBA sources to get correct pixel values.

	// Generic path — one draw.Draw call converts everything else
	// (RGBA, YCbCr, Gray, Paletted, NRGBA64, …)
	b := src.Bounds()
	dst := image.NewNRGBA(image.Rect(0, 0, b.Dx(), b.Dy()))
	draw.Draw(dst, dst.Bounds(), src, b.Min, draw.Src)
	return dst
}

// ─────────────────────────────────────────────────────────────────────
// lumaSamplerFor returns a closure that yields the luma (0-255) of a pixel,
// reading the cheapest representation the source offers. For a JPEG decode
// (*image.YCbCr) it reads the Y plane DIRECTLY — the Y plane already IS BT.601
// luma — so the perceptual hashes never allocate or convert a full-resolution
// RGBA buffer. Sampling only touches ~1024 pixels per image, so the per-pixel
// offset math (PixOffset/YOffset) is irrelevant to cost; eliminating the
// whole-image conversion is the win (benchmarked: −210 ms, −50 MB on a 12 MP
// image). The callback takes 0-based (px,py) in [0,W)×[0,H).
//
// Switching from "convert→RGB→recompute luma" to "read Y directly" shifts hash
// values by a hair (chroma rounding). All images in a scan use the same method,
// so near-duplicate matching stays internally consistent (and JPEG Y == the same
// 0.299/0.587/0.114 BT.601 luma the RGB paths compute, so cross-format still
// matches within threshold). A pre-existing cache from the old method is simply
// deleted and rebuilt — there is only one format.
func lumaSamplerFor(src image.Image) (sample func(px, py int) uint32, W, H int) {
	b := src.Bounds()
	W, H = b.Dx(), b.Dy()
	minX, minY := b.Min.X, b.Min.Y
	switch im := src.(type) {
	case *image.YCbCr:
		return func(px, py int) uint32 {
			return uint32(im.Y[im.YOffset(minX+px, minY+py)])
		}, W, H
	case *image.Gray:
		return func(px, py int) uint32 {
			return uint32(im.Pix[im.PixOffset(minX+px, minY+py)])
		}, W, H
	case *image.NRGBA:
		return func(px, py int) uint32 {
			o := im.PixOffset(minX+px, minY+py)
			r, g, bl := uint32(im.Pix[o]), uint32(im.Pix[o+1]), uint32(im.Pix[o+2])
			return (299*r + 587*g + 114*bl) / 1000
		}, W, H
	case *image.RGBA:
		return func(px, py int) uint32 {
			o := im.PixOffset(minX+px, minY+py)
			r, g, bl, a := uint32(im.Pix[o]), uint32(im.Pix[o+1]), uint32(im.Pix[o+2]), uint32(im.Pix[o+3])
			// RGBA Pix is premultiplied; un-premultiply a non-opaque pixel so its
			// luma matches the straight-alpha sources (matches the old toNRGBA path).
			// Opaque (a==255) is the common case and skips the divide.
			if a != 0 && a != 255 {
				r = r * 255 / a
				g = g * 255 / a
				bl = bl * 255 / a
			}
			return (299*r + 587*g + 114*bl) / 1000
		}, W, H
	default:
		// Rare formats (paletted, 16-bit, CMYK fallthrough): use the interface.
		return func(px, py int) uint32 {
			r, g, bl, _ := src.At(minX+px, minY+py).RGBA()
			return (299*(r>>8) + 587*(g>>8) + 114*(bl>>8)) / 1000
		}, W, H
	}
}

// ─────────────────────────────────────────────────────────────────────
// B1  dHashFast — 9×8 difference hash, luma sampled straight from the source
// ─────────────────────────────────────────────────────────────────────

func dHashFast(src image.Image) uint64 {
	const gw, gh = 9, 8
	sample, W, H := lumaSamplerFor(src)
	if W == 0 || H == 0 {
		return 0
	}

	var gray [gh][gw]uint32
	for y := 0; y < gh; y++ {
		for x := 0; x < gw; x++ {
			// Map to pixel coordinate (nearest-neighbour)
			px := x * W / gw
			py := y * H / gh
			if px >= W {
				px = W - 1
			}
			if py >= H {
				py = H - 1
			}
			gray[y][x] = sample(px, py)
		}
	}

	var hash uint64
	for y := 0; y < gh; y++ {
		for x := 0; x < 8; x++ {
			if gray[y][x] > gray[y][x+1] {
				hash |= 1 << uint(y*8+x)
			}
		}
	}
	return hash
}

// ─────────────────────────────────────────────────────────────────────
// B2  aHashFast — 8×8 average hash, luma sampled straight from the source
// ─────────────────────────────────────────────────────────────────────

func aHashFast(src image.Image) uint64 {
	sample, W, H := lumaSamplerFor(src)
	if W == 0 || H == 0 {
		return 0
	}

	var gray [64]uint32
	var sum uint32
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			px := x * W / 8
			py := y * H / 8
			if px >= W {
				px = W - 1
			}
			if py >= H {
				py = H - 1
			}
			luma := sample(px, py)
			gray[y*8+x] = luma
			sum += luma
		}
	}
	avg := sum / 64

	var hash uint64
	for i, v := range gray {
		if v > avg {
			hash |= 1 << uint(i)
		}
	}
	return hash
}

// ─────────────────────────────────────────────────────────────────────
// B3  pHashFast — 32×32 DCT perceptual hash, luma sampled from the source
// ─────────────────────────────────────────────────────────────────────

func pHashFast(src image.Image) uint64 {
	sample, W, H := lumaSamplerFor(src)
	if W == 0 || H == 0 {
		return 0
	}

	// Sample 32×32 grayscale straight from the decoded image
	var pixels [32][32]float64
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			px := x * W / 32
			py := y * H / 32
			if px >= W {
				px = W - 1
			}
			if py >= H {
				py = H - 1
			}
			pixels[y][x] = float64(sample(px, py))
		}
	}

	// 2D DCT: row pass
	var rowDCT [32][32]float64
	var rowIn [32]float64
	for y := 0; y < 32; y++ {
		rowIn = pixels[y]
		var rowOut [32]float64
		dct1d(&rowIn, &rowOut) // uses existing precomputed table
		rowDCT[y] = rowOut
	}

	// Column pass — only need top 8 rows of output
	var dct [8][8]float64
	var colIn [32]float64
	for x := 0; x < 8; x++ {
		for y := 0; y < 32; y++ {
			colIn[y] = rowDCT[y][x]
		}
		var colOut [32]float64
		dct1d(&colIn, &colOut)
		for y := 0; y < 8; y++ {
			dct[y][x] = colOut[y]
		}
	}

	// Median of 8×8, excluding DC at [0][0]
	coeffs := make([]float64, 0, 63)
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			if y == 0 && x == 0 {
				continue
			}
			coeffs = append(coeffs, dct[y][x])
		}
	}
	sort.Float64s(coeffs)
	median := coeffs[len(coeffs)/2]

	var hash uint64
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			if y == 0 && x == 0 {
				continue
			}
			if dct[y][x] > median {
				hash |= 1 << uint(y*8+x)
			}
		}
	}
	return hash
}

// ─────────────────────────────────────────────────────────────────────
// B3  resizeFitFast — nearest-neighbour resize using direct Pix access
// ─────────────────────────────────────────────────────────────────────

func resizeFitFast(src image.Image, maxW, maxH int) *image.NRGBA {
	b := src.Bounds()
	w, h := b.Dx(), b.Dy()
	if w == 0 || h == 0 {
		return image.NewNRGBA(image.Rect(0, 0, 1, 1))
	}

	// Compute output dimensions preserving aspect ratio
	nw, nh := w, h
	if w > maxW || h > maxH {
		scale := math.Min(float64(maxW)/float64(w), float64(maxH)/float64(h))
		nw = int(math.Round(float64(w) * scale))
		nh = int(math.Round(float64(h) * scale))
		if nw < 1 {
			nw = 1
		}
		if nh < 1 {
			nh = 1
		}
	}

	// Convert src to NRGBA once — zero-copy if already NRGBA
	srcN := toNRGBA(src)

	// Allocate output (fresh; pool is for the thumbnail handler's fixed size)
	dst := image.NewNRGBA(image.Rect(0, 0, nw, nh))
	srcW := srcN.Bounds().Dx()
	srcH := srcN.Bounds().Dy()

	// Pre-compute step ratios as fixed-point integers (16-bit fraction)
	// to avoid a float multiply inside the inner loop.
	stepX := (srcW << 16) / nw
	stepY := (srcH << 16) / nh

	for y := 0; y < nh; y++ {
		srcY := (y*stepY + 0x8000) >> 16 // round
		if srcY >= srcH {
			srcY = srcH - 1
		}
		srcRow := srcY * srcN.Stride
		dstRow := y * dst.Stride
		for x := 0; x < nw; x++ {
			srcX := (x*stepX + 0x8000) >> 16
			if srcX >= srcW {
				srcX = srcW - 1
			}
			si := srcRow + srcX*4
			di := dstRow + x*4
			dst.Pix[di] = srcN.Pix[si]
			dst.Pix[di+1] = srcN.Pix[si+1]
			dst.Pix[di+2] = srcN.Pix[si+2]
			dst.Pix[di+3] = srcN.Pix[si+3]
		}
	}
	return dst
}

// ─────────────────────────────────────────────────────────────────────
// B4  VP-tree — O(N log N) approximate nearest-neighbour for hashes
// ─────────────────────────────────────────────────────────────────────
//
// A Vantage Point Tree partitions a metric space around a chosen vantage
// point. Here the metric is Hamming distance on 64-bit hashes. Building
// is O(N log N), each query is O(log N), so finding all pairs within
// distance T is O(N log N) total instead of O(N²).
//
// Reference: Yianilos 1993, "Data Structures and Algorithms for
// Nearest Neighbor Search in General Metric Spaces".

type vpNode struct {
	idx    int     // index into the original images slice
	hash   uint64  // hash of this vantage point
	radius int     // partition radius
	left   *vpNode // images with dist ≤ radius
	right  *vpNode // images with dist > radius
}

// vpBuild builds a VP-tree over the given (index, hash) pairs.
func vpBuild(pts []vpPoint) *vpNode {
	if len(pts) == 0 {
		return nil
	}
	// Pick the first point as vantage point (random selection gives better
	// balance but determinism is more testable; swap strategies if needed).
	vp := pts[0]
	pts = pts[1:]
	if len(pts) == 0 {
		return &vpNode{idx: vp.idx, hash: vp.hash, radius: -1}
	}

	// Sort remaining points by distance from vantage point
	dists := make([]int, len(pts))
	for i, p := range pts {
		dists[i] = bits.OnesCount64(vp.hash ^ p.hash)
	}

	// Median distance = partition radius
	sorted := append([]int(nil), dists...)
	sort.Ints(sorted)
	median := sorted[len(sorted)/2]

	left := make([]vpPoint, 0, len(pts)/2+1)
	right := make([]vpPoint, 0, len(pts)/2+1)
	for i, p := range pts {
		if dists[i] <= median {
			left = append(left, p)
		} else {
			right = append(right, p)
		}
	}

	return &vpNode{
		idx:    vp.idx,
		hash:   vp.hash,
		radius: median,
		left:   vpBuild(left),
		right:  vpBuild(right),
	}
}

// vpPoint is the input type for the VP-tree.
type vpPoint struct {
	idx  int
	hash uint64
}

// vpSearch finds all indices within maxDist of query hash, appending to results.
func vpSearch(node *vpNode, query uint64, maxDist int, results *[]int) {
	if node == nil {
		return
	}
	d := bits.OnesCount64(node.hash ^ query)
	if d <= maxDist {
		*results = append(*results, node.idx)
	}
	if node.radius < 0 {
		return // leaf
	}
	// Prune: if the query ball [0, maxDist] overlaps the left subtree [0, radius]
	// or right subtree [radius+1, ∞], recurse into it.
	if d-maxDist <= node.radius { // might be in left
		vpSearch(node.left, query, maxDist, results)
	}
	if d+maxDist > node.radius { // might be in right
		vpSearch(node.right, query, maxDist, results)
	}
}

// findPerceptualGroupsVP groups images using a VP-tree query per hash type.
// For each active hash (dhash/ahash/phash), it builds a VP-tree and then
// runs one O(log N) query per image. Images that match on enough hashes
// (according to mode) are merged via union-find into groups.
func findPerceptualGroupsVP(
	rem []*ImageInfo,
	maxDist int,
	doDhash, doAhash, doPhash bool,
	mode string,
) [][]*ImageInfo {
	if len(rem) < 2 {
		return nil
	}

	// Union-Find (path-compressed, rank-based)
	parent := make([]int, len(rem))
	rank := make([]int, len(rem))
	for i := range parent {
		parent[i] = i
	}

	var findUF func(int) int
	findUF = func(x int) int {
		if parent[x] != x {
			parent[x] = findUF(parent[x]) // path compression
		}
		return parent[x]
	}
	unionUF := func(a, b int) {
		ra, rb := findUF(a), findUF(b)
		if ra == rb {
			return
		}
		if rank[ra] < rank[rb] {
			parent[ra] = rb
		} else if rank[ra] > rank[rb] {
			parent[rb] = ra
		} else {
			parent[rb] = ra
			rank[ra]++
		}
	}

	// For smart mode we need 2-of-3 hash types to agree.
	// Track per-pair match counts.
	type pairKey struct{ a, b int }
	matchCounts := make(map[pairKey]int) // only used in smart mode

	doOneHash := func(pts []vpPoint) {
		tree := vpBuild(append([]vpPoint(nil), pts...))
		neighbors := make([]int, 0, 16)
		for _, vp := range pts {
			neighbors = neighbors[:0]
			vpSearch(tree, vp.hash, maxDist, &neighbors)
			for _, ni := range neighbors {
				if ni == vp.idx {
					continue
				}
				a, b := vp.idx, ni
				if a > b {
					a, b = b, a
				}
				if mode == "smart" {
					k := pairKey{a, b}
					matchCounts[k]++
				} else {
					// Single-algo or all-must-agree: union immediately
					unionUF(a, b)
				}
			}
		}
	}

	if doDhash {
		pts := make([]vpPoint, len(rem))
		for i, img := range rem {
			pts[i] = vpPoint{i, img.DHash}
		}
		doOneHash(pts)
	}
	if doAhash {
		pts := make([]vpPoint, len(rem))
		for i, img := range rem {
			pts[i] = vpPoint{i, img.AHash}
		}
		doOneHash(pts)
	}
	if doPhash {
		pts := make([]vpPoint, len(rem))
		for i, img := range rem {
			pts[i] = vpPoint{i, img.PHash}
		}
		doOneHash(pts)
	}

	// Smart mode: union pairs that agree on ≥2 hash types
	if mode == "smart" {
		needed := 2
		for k, cnt := range matchCounts {
			if cnt >= needed {
				unionUF(k.a, k.b)
			}
		}
		// Release potentially large O(N²) map immediately
		matchCounts = nil
	}

	// Collect groups from union-find
	groupMap := make(map[int][]*ImageInfo, len(rem)/4)
	for i, img := range rem {
		root := findUF(i)
		groupMap[root] = append(groupMap[root], img)
	}

	groups := make([][]*ImageInfo, 0, len(groupMap))
	for _, g := range groupMap {
		if len(g) >= 2 {
			groups = append(groups, g)
		}
	}
	return groups
}

// ─────────────────────────────────────────────────────────────────────
// Thumbnail-generation background job
// ─────────────────────────────────────────────────────────────────────

// thumbTarget is one file to generate a thumbnail/poster for.
type thumbTarget struct {
	path    string
	isVideo bool
}

// collectThumbTargets returns the distinct image and video files across all
// duplicate groups. Other categories (audio/docs/archives) are excluded.
func collectThumbTargets(groups []*DuplicateGroup) []thumbTarget {
	seen := make(map[string]bool)
	var targets []thumbTarget
	for _, g := range groups {
		for _, im := range g.Images {
			ext := strings.ToLower(filepath.Ext(im.Path))
			isVid := videoExts[ext]
			if !allImgExts[ext] && !isVid {
				continue
			}
			if seen[im.Path] {
				continue
			}
			seen[im.Path] = true
			targets = append(targets, thumbTarget{path: im.Path, isVideo: isVid})
		}
	}
	return targets
}

// thumbConcurrency returns the ffmpeg (video-poster) subprocess cap and the
// per-ffmpeg thread cap for batch generation, matching runScan's subprocess-pool
// sizing so a big "generate all" stays near ~75% of cores and stays responsive.
// In-process image decodes are NOT bounded by this — they gate on thumbSem.
func thumbConcurrency() (subprocCap, videoThreads int) {
	numCPU := runtime.NumCPU()
	videoThreads = 2
	if numCPU <= 4 {
		videoThreads = 1
	}
	subprocCap = (numCPU * 3 / 4) / videoThreads
	if subprocCap < 1 {
		subprocCap = 1
	}
	if subprocCap > 8 {
		subprocCap = 8
	}
	return subprocCap, videoThreads
}

// runThumbGen generates image thumbnails and video frame-posters for every
// image/video file in the current results, writing them to the disk thumb cache.
// It mirrors runScan: one job at a time, live progress under state.mu, and a
// cancelable context (cancellation via thumbCancelFn kills in-flight ffmpeg;
// otherwise each ffmpeg call is bounded by extractTimeout). It does NOT reset
// cancelThumb (the start handler does that before launching), so it stays unit-testable.
func runThumbGen() {
	state.mu.RLock()
	groups := make([]*DuplicateGroup, len(state.groups))
	copy(groups, state.groups)
	state.mu.RUnlock()
	targets := collectThumbTargets(groups)

	ctx, cancel := context.WithCancel(context.Background())
	thumbCancelMu.Lock()
	thumbCancelFn = cancel
	thumbCancelMu.Unlock()
	defer func() {
		cancel()
		thumbCancelMu.Lock()
		thumbCancelFn = nil
		thumbCancelMu.Unlock()
		state.mu.Lock()
		state.thumbing = false
		state.mu.Unlock()
	}()

	ensureThumbCacheDir()
	ffmpegOK := videoToolsAvailable()
	// Images decode in-process and gate on thumbSem, so run as many pool workers
	// as there are decode slots (≫ the old cap of 8) for fast bulk thumbnailing.
	// Video posters shell out to ffmpeg, so they stay throttled by a separate
	// subprocess semaphore (subprocCap) to keep the machine responsive.
	subprocCap, videoThreads := thumbConcurrency()
	workers := cap(thumbSem)
	if workers < subprocCap {
		workers = subprocCap
	}
	subprocSem := make(chan struct{}, subprocCap)
	if ffmpegOK {
		// Safe to leave set: scan/thumbgen are mutually exclusive and runScan re-initializes this at its start.
		setVideoThreadLimit(videoThreads)
	}

	atomic.StoreInt64(&thumbDone, 0)
	atomic.StoreInt64(&thumbFailed, 0)
	atomic.StoreInt64(&thumbSkipped, 0)

	state.mu.Lock()
	state.thumbProgress = ThumbProgress{
		Total:   int64(len(targets)),
		Status:  "Generating thumbnails…",
		Running: true,
	}
	state.mu.Unlock()

	// Snapshot atomic counters into state.thumbProgress periodically.
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(300 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				state.mu.Lock()
				state.thumbProgress.Done = atomic.LoadInt64(&thumbDone)
				state.thumbProgress.Failed = atomic.LoadInt64(&thumbFailed)
				state.thumbProgress.Skipped = atomic.LoadInt64(&thumbSkipped)
				state.mu.Unlock()
			}
		}
	}()

	work := make(chan thumbTarget)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tgt := range work {
				if atomic.LoadInt64(&cancelThumb) == 1 {
					continue // drain remaining quickly without doing work
				}
				stat, err := os.Stat(tgt.path)
				if err != nil {
					atomic.AddInt64(&thumbSkipped, 1)
					continue
				}
				dst := thumbCachePath(tgt.path, stat.Size(), stat.ModTime().UnixNano())
				if _, err := os.Stat(dst); err == nil {
					atomic.AddInt64(&thumbDone, 1) // already cached → idempotent skip
					continue
				}
				if tgt.isVideo {
					if !ffmpegOK {
						atomic.AddInt64(&thumbSkipped, 1)
						continue
					}
					subprocSem <- struct{}{} // throttle concurrent ffmpeg processes
					ok := extractFramePoster(ctx, tgt.path, dst)
					<-subprocSem
					if ok {
						atomic.AddInt64(&thumbDone, 1)
					} else {
						atomic.AddInt64(&thumbFailed, 1)
					}
					continue
				}
				data, err := encodeImageThumb(tgt.path)
				if err != nil {
					atomic.AddInt64(&thumbFailed, 1)
					continue
				}
				if err := writeThumbAtomic(dst, data); err != nil {
					atomic.AddInt64(&thumbFailed, 1)
					continue
				}
				atomic.AddInt64(&thumbDone, 1)
			}
		}()
	}

	for _, tgt := range targets {
		if atomic.LoadInt64(&cancelThumb) == 1 {
			break
		}
		work <- tgt
	}
	close(work)
	wg.Wait()
	close(stop)

	state.mu.Lock()
	p := &state.thumbProgress
	p.Done = atomic.LoadInt64(&thumbDone)
	p.Failed = atomic.LoadInt64(&thumbFailed)
	p.Skipped = atomic.LoadInt64(&thumbSkipped)
	p.Running = false
	p.Complete = true
	switch {
	case atomic.LoadInt64(&cancelThumb) == 1:
		p.Status = "Cancelled"
	case !ffmpegOK && p.Skipped > 0:
		p.Status = fmt.Sprintf("Done — %d generated, %d failed, %d skipped (ffmpeg not found)", p.Done, p.Failed, p.Skipped)
	default:
		p.Status = fmt.Sprintf("Done — %d generated, %d failed", p.Done, p.Failed)
	}
	status := p.Status
	state.mu.Unlock()
	logf("INFO", "Thumbnail generation finished: %s", status)
}
