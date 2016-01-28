package main

import (
	"fmt"
	"os"
)

var (
	// Debug is true when application is started in debug mode (--debug)
	Debug     = false
	errorsCnt int
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
	errorsCnt++
}

// LogFatal display error messages on stderr and exit
func LogFatal(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	fmt.Fprintln(os.Stderr)
	os.Exit(-2)
}

// AnyErrors return true when any error was logged by LogError
func AnyErrors() bool {
	return errorsCnt > 0
}

// ExitWhenErrors stop application when any error was logged by LogError
func ExitWhenErrors() {
	if AnyErrors() {
		os.Exit(-1)
	}
}

// InList return true when value exist in list
func InList(value int, list []int) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}
