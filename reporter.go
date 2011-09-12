package dmrgo

import (
	"os"
	"fmt"
)

func Reportln(a ...interface{}) {
	s := fmt.Sprintln(a...)
	fmt.Fprintf(os.Stderr, "reporter:status:%s", s) // \n is in s
}

func Reportf(format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...) // we should check if s contains \n
	fmt.Fprintf(os.Stderr, "reporter:status:%s\n", s)
}

func IncrCounter(group, counter string, amount int) {
	fmt.Fprintf(os.Stderr, "reporter:counter:%s,%s,%d\n", group, counter, amount)
}
