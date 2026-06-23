//go:build !windows

package dup

import "os/exec"

// setLowPriority is a no-op on non-Windows platforms. (A nice() equivalent could
// be added per-OS later; on macOS/Linux ffmpeg already yields more gracefully.)
func setLowPriority(cmd *exec.Cmd) {}
