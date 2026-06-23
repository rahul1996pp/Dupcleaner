@echo off
REM ====================================================================
REM  bench.bat - benchmark DupCleaner across all file types
REM  Usage:  bench.bat [real-files-dir]
REM            real-files-dir  benchmark against your own files
REM                            (optional; synthetic media used if omitted)
REM
REM  Scans images, videos, and audio across a range of worker counts,
REM  measures peak RAM at each, and prints a report recommending the best
REM  Threads (CPU) and RAM for each file type on THIS machine.
REM  Needs ffmpeg on PATH (used to build throwaway test media).
REM
REM  Advanced: set DUPCLEANER_BENCH_LIMIT=<n> for files/category (default 80),
REM            or DUPCLEANER_BENCH_CATS=images,videos to limit categories.
REM ====================================================================
setlocal

where go >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Go is not installed or not on PATH.
    exit /b 1
)

where ffmpeg >nul 2>nul
if errorlevel 1 (
    echo [WARN] ffmpeg not on PATH - video/audio benchmarks will be skipped.
)

echo Benchmarking all file types (this can take several minutes)...
echo.
set "DUPCLEANER_BENCH=1"
set "DUPCLEANER_BENCH_DIR=%~1"
go test -run="^TestStressBenchmark$" -v -timeout 30m
if errorlevel 1 (
    echo [ERROR] Benchmark run failed.
    exit /b 1
)
endlocal
