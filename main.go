// Command dupcleaner is a thin entry point. All logic lives in the internal/dup
// package so that the package's tests can sit beside the code they exercise
// while keeping the repository root tidy.
package main

import "dupcleaner/internal/dup"

func main() {
	dup.Main()
}
