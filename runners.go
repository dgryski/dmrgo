// Protocols for un/marshaling stream values
// Copyright (c) 2011 Damian Gryski <damian@gryski.com>
// License: GPLv3 or, at your option, any later version

package dmrgo

import (
	"bufio"
	"flag"
	"fmt"
	"hash/adler32"
	"io"
	"os"
	"strings"
)

// KeyValue is the primary type for interacting with Hadoop.
type KeyValue struct {
	Key   string
	Value string
}

func readLineValue(br *bufio.Reader) (*KeyValue, error) {
	s, err := br.ReadString('\n')
	s = strings.TrimRight(s, "\n")
	if err != nil {
		return nil, err
	}
	return &KeyValue{"", s}, err
}

func readLineKeyValue(br *bufio.Reader) (*KeyValue, error) {

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

type Emitter interface {
	Emit(key string, value string)
}

type printEmitter struct {
	w *bufio.Writer
}

func newPrintEmitter(w *bufio.Writer) *printEmitter {
	e := new(printEmitter)
	e.w = w
	return e
}

func (e *printEmitter) Emit(key string, value string) {
        e.w.WriteString(key)
        e.w.WriteString("\t")
        e.w.WriteString(value)
        e.w.WriteString("\n")
}

func (e *printEmitter) Flush() {
	e.w.Flush()
}

type partitionEmitter struct {
	partitions       uint32
	FileNames        []string
	writers          []*bufio.Writer
	fds              []*os.File
	fileNameTemplate string
}

func newPartitionEmitter(partitions uint, template string) *partitionEmitter {
	pe := new(partitionEmitter)
	pe.partitions = uint32(partitions)
	pe.fileNameTemplate = template
	pe.FileNames = make([]string, partitions)
	pe.writers = make([]*bufio.Writer, partitions)
	pe.fds = make([]*os.File, partitions)
	return pe
}

func (e *partitionEmitter) Emit(key string, value string) {

	partition := uint32(0)

	if e.partitions > 1 {
		partition = adler32.Checksum([]byte(key)) % uint32(e.partitions)
	}

	if e.writers[partition] == nil {
		e.FileNames[partition] = fmt.Sprintf("%s.%04d", e.fileNameTemplate, partition)
		fd, _ := os.Create(e.FileNames[partition])
		e.fds[partition] = fd
		e.writers[partition] = bufio.NewWriter(fd)
	}

	fmt.Fprintf(e.writers[partition], "%s\t%s\n", key, value)
}

func (e *partitionEmitter) Flush() {
	for _, w := range e.writers {
		w.Flush()
	}
}

func (e *partitionEmitter) Close() {
	for _, w := range e.fds {
		w.Close()
	}
}

// MapReduceJob is the interface expected by the job runner
type MapReduceJob interface {
	Map(key string, value string, emitter Emitter)

	// Called at the end of the Map phase 
	MapFinal(emitter Emitter)

	Reduce(key string, values []string, emitter Emitter)
}

// are in we in the map or reduce phase?
var doMap bool
var doReduce bool
var emitPartitions int
var doMapReduce bool

func init() {
	flag.BoolVar(&doMap, "mapper", false, "run mapper code on stdin")
	flag.BoolVar(&doReduce, "reducer", false, "run reducer on stdin")
	flag.IntVar(&emitPartitions, "partitions", 1, "parition data into sets")
	flag.BoolVar(&doMapReduce, "mapreduce", false, "run full map/reduce")
}

func mapreduce(mrjob MapReduceJob) {

	mEmit := newPartitionEmitter(uint(emitPartitions), fmt.Sprintf("tmp-map-out-p%d", os.Getpid()))
	rEmit := newPrintEmitter(bufio.NewWriter(os.Stdout))
	attr := new(os.ProcAttr)
	attr.Files = []*os.File{nil, nil, nil}

	mapper(mrjob, os.Stdin, mEmit)

	mEmit.Flush()
	mEmit.Close()

	for _, fn := range mEmit.FileNames {
		if fn == "" {
			continue
		}

		// sort
		p, err := os.StartProcess("/usr/bin/sort", []string{"sort", fn, "-o", "tmp-red-in.0000"}, attr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "err running sort: ", err)
		}
		p.Wait(0)

		// reduce
		f, _ := os.Open("tmp-red-in.0000")
		reducer(mrjob, f, rEmit)
		os.Remove(fn)
	}
	os.Remove("tmp-red-in.0000")
	rEmit.Flush()
}

// Main runs the map reduce job passed in
func Main(mrjob MapReduceJob) {

	if doMapReduce {
		mapreduce(mrjob)
		return
	}

	if doMap && doReduce {
		fmt.Println("can either map or reduce, not both")
		os.Exit(1)
	}

	if !doMap && !doReduce {
		fmt.Println("neither map nor reduce called")
		os.Exit(1)
	}

	stdout := bufio.NewWriter(os.Stdout)

	emitter := newPrintEmitter(stdout)

	if doMap {
		mapper(mrjob, os.Stdin, emitter)
	}

	if doReduce {
		reducer(mrjob, os.Stdin, emitter)
	}

	emitter.Flush()
}

// run the mapping phase, calling the map routine on key/value pairs on stdin and writing the results to stdout
func mapper(mrjob MapReduceJob, r io.Reader, emitter Emitter) {

	br := bufio.NewReader(r)

	for {
		kv, err := readLineValue(br)
		if err != nil {
			break
		}

		mrjob.Map("", kv.Value, emitter)

	}

	// handle any finalization from the mapper
	mrjob.MapFinal(emitter)
}

// run the mapping phase, calling the reduce routine on key/[]value read from stdin and writing the results to stdout
// We aggregate the values that have been mapped with the same key, then call the users Reduce function
func reducer(mrjob MapReduceJob, r io.Reader, emitter Emitter) {

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
				mrjob.Reduce(currentKey, values, emitter)
				values = []string{}
			}
			currentKey = mkv.Key
			values = append(values, mkv.Value)
		}
	}

	// final reducer call with pending 'values'
	mrjob.Reduce(currentKey, values, emitter)
}
