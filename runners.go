// Protocols for un/marshaling stream values
// Copyright (c) 2011 Damian Gryski <damian@gryski.com>
// License: GPLv3 or, at your option, any later version

package dmrgo

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"flag"
)

// KeyValue is the primary type for interacting with Hadoop.
type KeyValue struct {
	Key   string
	Value string
}

func readLineValue(br *bufio.Reader) (*KeyValue, os.Error) {
	s, err := br.ReadString('\n')
	s = strings.TrimRight(s, "\n")
	if err != nil {
		return nil, err
	}
	return &KeyValue{"", s}, err
}

func readLineKeyValue(br *bufio.Reader) (*KeyValue, os.Error) {

	k, err := br.ReadString('\t')
	if err != nil {
		return nil, err
	}

	v, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}

	k = strings.TrimRight(k, "\t")
	v = strings.TrimRight(v, "\n")

	return &KeyValue{k, v}, nil
}

// MapReduceJob is the interface expected by the job runner
type MapReduceJob interface {

	Map(key string, value string) []*KeyValue

        // Called at the end of the Map phase 
	MapFinal() []*KeyValue

	Reduce(key string, values []string) []*KeyValue
}
 
// are in we in the map or reduce phase?
var doMap bool
var doReduce bool

func init() {
	flag.BoolVar(&doMap, "mapper", false, "run mapper code on stdin")
	flag.BoolVar(&doReduce, "reducer", false, "run reducer on stdin")
}

// Main runs the map reduce job passed in
func Main(mrjob MapReduceJob) {

	if doMap && doReduce {
		fmt.Println("can either map or reduce, not both")
		os.Exit(1)
	}

	if !doMap && !doReduce {
		fmt.Println("neither map not reduce called")
		os.Exit(1)
	}

	if doMap {
		mapper(mrjob, os.Stdin, os.Stdout)
	}

	if doReduce {
		reducer(mrjob, os.Stdin, os.Stdout)
	}
}

// run the mapping phase, calling the map routine on key/value pairs on stdin and writing the results to stdout
func mapper(mrjob MapReduceJob, r io.Reader, w io.Writer) {

	br := bufio.NewReader(r)

	for {
		kv, err := readLineValue(br)
		if err != nil {
			break
		}

		kvs := mrjob.Map("", kv.Value)

		for _, kv := range kvs {
			fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
		}
	}

	// handle any finalization from the mapper
	kvs := mrjob.MapFinal()
	for _, kv := range kvs {
		fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
	}

}

// run the mapping phase, calling the reduce routine on key/[]value read from stdin and writing the results to stdout
// We aggregate the values that have been mapped with the same key, then call the users Reduce function
func reducer(mrjob MapReduceJob, r io.Reader, w io.Writer) {

	br := bufio.NewReader(r)

	var currentKey string
	values := []string{}

	for {
		mkv, err := readLineKeyValue(br)
		if err != nil {
			break
		}

		if currentKey == mkv.Key {
			values = append(values, mkv.Value)
		} else {
			if currentKey != "" {
				rkvs := mrjob.Reduce(currentKey, values)
				for _, kv := range rkvs {
					fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
				}
				values = []string{}
			}
			currentKey = mkv.Key
			values = append(values, mkv.Value)
		}
	}

        // final reducer call with pending 'values'
        rkvs := mrjob.Reduce(currentKey, values)
        for _, kv := range rkvs {
                fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
        }
}
