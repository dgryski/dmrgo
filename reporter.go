// Update status and counters to the Hadoop framework
// Copyright (c) 2011 Damian Gryski <damian@gryski.com>
// License: GPLv3 or, at your option, any later versiono

package dmrgo

import (
	"os"
	"fmt"
)

// Statusln updates the Hadoop job status.  The arguments are passed to fmt.Sprintln
func Statusln(a ...interface{}) {
	s := fmt.Sprintln(a...)
	fmt.Fprintf(os.Stderr, "reporter:status:%s", s) // \n is in s
}

// Statusf updates the Hadoop job status.  The arguments are passed to fmt.Sprintf
func Statusf(format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...) // we should check if s contains \n
	fmt.Fprintf(os.Stderr, "reporter:status:%s\n", s)
}

// IncrCounter updates the given group/counter by 'amount'
func IncrCounter(group, counter string, amount int) {
	fmt.Fprintf(os.Stderr, "reporter:counter:%s,%s,%d\n", group, counter, amount)
}
