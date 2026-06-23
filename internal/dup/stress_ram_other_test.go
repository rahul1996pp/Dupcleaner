//go:build !windows

package dup

// procTreeWorkingSetBytes is Windows-only; on other platforms it returns 0 so
// the benchmark falls back to Go heap in-use (see benchPeakRAM).
func procTreeWorkingSetBytes() uint64 { return 0 }
