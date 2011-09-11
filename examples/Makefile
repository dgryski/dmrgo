include $(GOROOT)/src/Make.inc

TARG=wordcount
GOFILES=\
	wordcount.go

include $(GOROOT)/src/Make.cmd

PWD=$(shell pwd)

wordcount_test:	wordcount data.in
		cat data.in | ./wordcount --mapper |sort |./wordcount --reducer >data.out
		@[ "6aef73971d21f5c27cffb9c36b0517cc  data.out" = "`md5sum data.out`" ] || echo "Fail"

wordcount_test_hadoop:	wordcount data.in
	/usr/bin/hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-*streaming*.jar  -mapper "${PWD}/wordcount --mapper" -reducer "${PWD}/wordcount --reducer" -input "${PWD}/data.in" -output /tmp/mrtest/mrtest-output

data.in:
	cat /usr/share/common-licenses/GPL-3 | tr A-Z a-z |tr -c "a-z \n" ' ' >data.in
