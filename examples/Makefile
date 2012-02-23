TARG=wordcount
GOFILES=\
	wordcount.go

PWD=$(shell pwd)
INFILE=/usr/share/common-licenses/GPL-3

wordcount:	wordcount.go
	go build -o $@

wordcount_test:	wordcount data.in wordcount_test_tr
		cat data.in | ./wordcount --mapper |sort |./wordcount --reducer >data.out
		./wordcount --mapreduce data.in
		md5sum `ls -rt red-out-p*.0000 |tail -1` data.out wordcount-tr.txt
		
wordcount_test_hadoop:	wordcount data.in
	${HADOOP_HOME}/bin/hadoop fs -ls data.in && ${HADOOP_HOME}/bin/hadoop fs -rm data.in
	${HADOOP_HOME}/bin/hadoop fs -put data.in data.in
	${HADOOP_HOME}/bin/hadoop fs -test -d data.out && ${HADOOP_HOME}/bin/hadoop fs -rmr data.out
	${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-*streaming*.jar -verbose -mapper "${PWD}/wordcount --mapper" -reducer "${PWD}/wordcount --reducer" -input "data.in" -output "data.out"
	rm -rf data.out
	${HADOOP_HOME}/bin/hadoop fs -get data.out data.out
	md5sum data.out/part-00000

wordcount_test_tr:
	cat $(INFILE) |tr 'A-Z' 'a-z' |tr -c 'a-z\n' ' ' |tr ' ' '\n' |grep -v '^$$' |sort |uniq -c |awk '{print $$2 "\t" $$1;}' >wordcount-tr.txt

data.in:
	cp $(INFILE) data.in
