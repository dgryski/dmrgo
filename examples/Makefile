include $(GOROOT)/src/Make.inc

TARG=wordcount
GOFILES=\
	wordcount.go

include $(GOROOT)/src/Make.cmd

PWD=$(shell pwd)

wordcount_test:	wordcount data.in
		cat data.in | ./wordcount --mapper |sort |./wordcount --reducer >data.out
		@[ "945bdd73efc277a8830305c604f3e54e  data.out" = "`md5sum data.out`" ] || echo "Fail"

wordcount_test_hadoop:	wordcount data.in
	${HADOOP_HOME}/bin/hadoop fs -ls data.in && ${HADOOP_HOME}/bin/hadoop fs -rm data.in
	${HADOOP_HOME}/bin/hadoop fs -put data.in data.in
	${HADOOP_HOME}/bin/hadoop fs -test -d data.out && ${HADOOP_HOME}/bin/hadoop fs -rmr data.out
	${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-*streaming*.jar -verbose -mapper "${PWD}/wordcount --mapper" -reducer "${PWD}/wordcount --reducer" -input "data.in" -output "data.out"
	rm -rf data.out
	${HADOOP_HOME}/bin/hadoop fs -get data.out data.out
	md5sum data.out/part-00000

data.in:
	cat /usr/share/common-licenses/GPL-3 | tr A-Z a-z |tr -c "a-z \n" ' ' >data.in
