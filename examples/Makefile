include $(GOROOT)/src/Make.inc

TARG=wordcount
GOFILES=\
	wordcount.go

include $(GOROOT)/src/Make.cmd

wordcount_test:	wordcount
		echo "hello bar world foo bar baz qux hello" | ./wordcount --mapper |sort |./wordcount --reducer
