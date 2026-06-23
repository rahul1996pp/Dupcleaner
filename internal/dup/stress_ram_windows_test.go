//go:build windows

package dup

import (
	"syscall"
	"unsafe"
)

// Windows process-tree working-set measurement for the benchmark. A Toolhelp
// snapshot maps the process tree and psapi's GetProcessMemoryInfo reads each
// process's resident memory. This is fast enough to poll in-process, so it
// doesn't spawn helpers (unlike wmic/tasklist) that would pollute the very tree
// it is measuring.

const (
	_TH32CS_SNAPPROCESS        = 0x00000002
	_PROCESS_QUERY_INFORMATION = 0x0400
	_PROCESS_VM_READ           = 0x0010
)

var (
	modpsapi                 = syscall.NewLazyDLL("psapi.dll")
	procGetProcessMemoryInfo = modpsapi.NewProc("GetProcessMemoryInfo")
)

// processMemoryCounters mirrors the Win32 PROCESS_MEMORY_COUNTERS struct.
type processMemoryCounters struct {
	cb                         uint32
	pageFaultCount             uint32
	peakWorkingSetSize         uintptr
	workingSetSize             uintptr
	quotaPeakPagedPoolUsage    uintptr
	quotaPagedPoolUsage        uintptr
	quotaPeakNonPagedPoolUsage uintptr
	quotaNonPagedPoolUsage     uintptr
	pagefileUsage              uintptr
	peakPagefileUsage          uintptr
}

// procTreeWorkingSetBytes sums the working set of the current process and every
// descendant (the ffmpeg/ffprobe/fpcalc children a scan spawns). Returns 0 if
// the snapshot can't be taken, signalling the caller to fall back to heap.
func procTreeWorkingSetBytes() uint64 {
	parents, ok := snapshotParents()
	if !ok {
		return 0
	}
	tree := descendantsOf(uint32(syscall.Getpid()), parents)
	var total uint64
	for pid := range tree {
		total += processWorkingSet(pid)
	}
	return total
}

// snapshotParents returns pid -> parent-pid for every running process.
func snapshotParents() (map[uint32]uint32, bool) {
	snap, err := syscall.CreateToolhelp32Snapshot(_TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return nil, false
	}
	defer syscall.CloseHandle(snap)

	var e syscall.ProcessEntry32
	e.Size = uint32(unsafe.Sizeof(e))
	if err := syscall.Process32First(snap, &e); err != nil {
		return nil, false
	}
	parents := make(map[uint32]uint32, 256)
	for {
		parents[e.ProcessID] = e.ParentProcessID
		if err := syscall.Process32Next(snap, &e); err != nil {
			break // ERROR_NO_MORE_FILES ends the walk
		}
	}
	return parents, true
}

// descendantsOf returns root plus every process reachable from it through the
// parent links. It walks downward from root so a stale or reused parent PID
// can't drag an unrelated ancestor into the set.
func descendantsOf(root uint32, parents map[uint32]uint32) map[uint32]bool {
	children := make(map[uint32][]uint32, len(parents))
	for pid, ppid := range parents {
		children[ppid] = append(children[ppid], pid)
	}
	set := map[uint32]bool{root: true}
	stack := []uint32{root}
	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, c := range children[p] {
			if !set[c] {
				set[c] = true
				stack = append(stack, c)
			}
		}
	}
	return set
}

// processWorkingSet returns one process's working-set bytes, or 0 if it can't
// be opened (e.g. it exited between the snapshot and the query).
func processWorkingSet(pid uint32) uint64 {
	h, err := syscall.OpenProcess(_PROCESS_QUERY_INFORMATION|_PROCESS_VM_READ, false, pid)
	if err != nil {
		return 0
	}
	defer syscall.CloseHandle(h)
	var pmc processMemoryCounters
	pmc.cb = uint32(unsafe.Sizeof(pmc))
	r, _, _ := procGetProcessMemoryInfo.Call(uintptr(h), uintptr(unsafe.Pointer(&pmc)), uintptr(pmc.cb))
	if r == 0 {
		return 0
	}
	return uint64(pmc.workingSetSize)
}
