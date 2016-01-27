package main

import (
	"fmt"
	"os"
)

var (
	Debug         = false
	errorsCnt int = 0
)

// LogDebug display debugging information on stderr
func LogDebug(format string, a ...interface{}) {
	if Debug {
		fmt.Fprintf(os.Stderr, format, a...)
		fmt.Fprintln(os.Stderr)
	}
}

// Log display standard messages on stderr
func Log(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	fmt.Fprintln(os.Stderr)
}

// LogError display error messages on stderr and continue
func LogError(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	fmt.Fprintln(os.Stderr)
	errorsCnt += 1
}

// LogError display error messages on stderr and exit
func LogFatal(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	fmt.Fprintln(os.Stderr)
	os.Exit(-2)
}

func AnyErrors() bool {
	return errorsCnt > 0
}

func ExitWhenErrors() {
	if AnyErrors() {
		os.Exit(-1)
	}
}

func InList(value int, list []int) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}
