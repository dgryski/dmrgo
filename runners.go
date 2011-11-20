// Logic for running our map/reduce jobs
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
	"path/filepath"
	"runtime"
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
	Flush()
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
	e.w.WriteByte('\t')
	e.w.WriteString(value)
	e.w.WriteByte('\n')
}

func (e *printEmitter) Flush() {
	e.w.Flush()
}

type partitionEmitter struct {
	partitions       uint32
	FileNames        []string
	fds              []*os.File
	emitters         []Emitter
	fileNameTemplate string
}

// data sink -- useful for benchmarking
type nullEmitter struct{}

func (*nullEmitter) Emit(key string, value string) { /* nothing */
}
func (*nullEmitter) Flush() { /* nothing */
}

func newPartitionEmitter(partitions uint, template string) *partitionEmitter {
	pe := new(partitionEmitter)
	pe.partitions = uint32(partitions)
	pe.fileNameTemplate = template
	pe.FileNames = make([]string, partitions)
	pe.fds = make([]*os.File, partitions)
	pe.emitters = make([]Emitter, partitions)
	return pe
}

func (e *partitionEmitter) Emit(key string, value string) {

	partition := uint32(0)

	if e.partitions > 1 {
		partition = adler32.Checksum([]byte(key)) % uint32(e.partitions)
	}

	if e.emitters[partition] == nil {
		e.FileNames[partition] = fmt.Sprintf("%s.%04d", e.fileNameTemplate, partition)
		fd, _ := os.Create(e.FileNames[partition])
		e.fds[partition] = fd
		w := bufio.NewWriter(fd)
		e.emitters[partition] = newPrintEmitter(w)
	}

	e.emitters[partition].Emit(key, value)
}

func (e *partitionEmitter) Flush() {
	for _, w := range e.emitters {
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
var optDoMap bool
var optDoReduce bool
var optNumPartitions int
var optDoMapReduce bool
var optNumMappers int

func init() {
	flag.BoolVar(&optDoMap, "mapper", false, "run mapper code on stdin")
	flag.BoolVar(&optDoReduce, "reducer", false, "run reducer on stdin")
	flag.IntVar(&optNumPartitions, "partitions", 1, "parition data into sets")
	flag.BoolVar(&optDoMapReduce, "mapreduce", false, "run full map/reduce")
	flag.IntVar(&optNumMappers, "mappers", 4, "number of map processes")
}

func mapreduce(mrjob MapReduceJob) {

	attr := new(os.ProcAttr)
	attr.Files = []*os.File{nil, nil, nil}

	pid := os.Getpid()

	runtime.GOMAXPROCS(optNumMappers + 1)
	sem := make(chan int, optNumMappers) // handle 'mappers' concurrent processors
	done := make(chan int)               // signal channel to make sure everybody has completed

	mapperInputs := flag.Args()

	// no input files -- read from stdin
	if len(mapperInputs) == 0 {
		mEmit := newPartitionEmitter(uint(optNumPartitions), fmt.Sprintf("tmp-map-out-p%d-f0", pid))
		mapper(mrjob, os.Stdin, mEmit)
		mapper_final(mrjob, mEmit)
		mEmit.Flush()
		mEmit.Close()
		mapperInputs = []string{"(stdin)"}
	} else {
		// we have multiple input files -- run up to 'mappers' of them in parallel

		for i, fn := range mapperInputs {
			go func(i int, fn string) {
				sem <- 1
				defer func() { <-sem; done <- 1 }()

				f, err := os.Open(fn)
				if err != nil {
					fmt.Fprintln(os.Stderr, "err opening ", f, ": ", err)
					return
				}

				mEmit := newPartitionEmitter(uint(optNumPartitions), fmt.Sprintf("tmp-map-out-p%d-f%d", pid, i))
				mapper(mrjob, f, mEmit)
				mEmit.Flush()
				mEmit.Close()
				f.Close()
			}(i, fn)
		}

		// wait for mappers to finish
		for i := 0; i < len(mapperInputs); i++ {
			<-done
		}

		// then launch mapper_final
		mEmit := newPartitionEmitter(uint(optNumPartitions), fmt.Sprintf("tmp-map-out-p%d-f%d", pid, len(mapperInputs)))
		mapper_final(mrjob, mEmit)
		mEmit.Flush()
		mEmit.Close()
	}

	for i := 0; i < optNumPartitions; i++ {

		go func(i int) {

			sem <- 1
			defer func() { <-sem; done <- 1 }()

			fns, _ := filepath.Glob(fmt.Sprintf("tmp-map-out-p%d-f*.%04d", pid, i))

			redin := fmt.Sprintf("tmp-red-in-p%d.%04d", pid, i)

			cmdline := []string{"sort", "-o", redin}
			cmdline = append(cmdline, fns...)

			// sort
			p, err := os.StartProcess("/usr/bin/sort", cmdline, attr)
			if err != nil {
				fmt.Fprintln(os.Stderr, "err running sort: ", err)
			}
			p.Wait(0)

			// reduce
			f, _ := os.Open(redin)
			rout, _ := os.Create(fmt.Sprintf("red-out-p%d.%04d", pid, i))
			rEmit := newPrintEmitter(bufio.NewWriter(rout))
			reducer(mrjob, f, rEmit)
			for _, fn := range fns {
				os.Remove(fn)
			}
			os.Remove(redin)
			rEmit.Flush()
			rout.Close()
		}(i)
	}

	// wait for reducers to finish
	for i := 0; i < optNumPartitions; i++ {
		<-done
		// stdout? then cat just-created file then unlink it
	}

	if optNumPartitions == 1 {
		fmt.Printf("output is in: red-out-p%d.0000\n", pid)
	} else {
		fmt.Printf("output is in: red-out-p%d.0000 - red-out-p%d.%04d\n", pid, pid, optNumPartitions-1)
	}
}

// Main runs the map reduce job passed in
func Main(mrjob MapReduceJob) {

	if optDoMapReduce {
		mapreduce(mrjob)
		return
	}

	if optDoMap && optDoReduce {
		fmt.Println("can either map or reduce, not both. (Did  you mean --mapreduce ?)")
		os.Exit(1)
	}

	if !optDoMap && !optDoReduce {
		fmt.Println("neither map nor reduce called")
		os.Exit(1)
	}

	stdout := bufio.NewWriter(os.Stdout)

	emitter := newPrintEmitter(stdout)

	if optDoMap {
		mapper(mrjob, os.Stdin, emitter)
		// handle any finalization from the mapper
		mapper_final(mrjob, emitter)
	}

	if optDoReduce {
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
}

// run the cleanup phase for the mapper
func mapper_final(mrjob MapReduceJob, emitter Emitter) {
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
