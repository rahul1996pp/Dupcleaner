//go:build windows

package dup

import (
	"os/exec"
	"syscall"
)

// Windows process-creation flags (not exported by the syscall package).
const (
	belowNormalPriorityClass = 0x00004000 // BELOW_NORMAL_PRIORITY_CLASS
	createNoWindow           = 0x08000000 // CREATE_NO_WINDOW
)

// setLowPriority makes a child process (ffmpeg/ffprobe/fpcalc) run below normal
// priority and without flashing a console window. Below-normal priority lets the
// OS scheduler keep the UI and other apps responsive while a batch of decoders
// saturates the cores — the decode work still gets all otherwise-idle CPU, so it
// is not meaningfully slower, but the machine no longer feels "stuck".
func setLowPriority(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= belowNormalPriorityClass | createNoWindow
}
