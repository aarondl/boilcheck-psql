package main

import (
	"fmt"
	"os"
)

func debugln(args ...interface{}) {
	if !flagDebug {
		return
	}

	fmt.Fprintln(os.Stderr, args...)
}

func debugf(format string, args ...interface{}) {
	if !flagDebug {
		return
	}
	fmt.Fprintf(os.Stderr, format, args...)
}
