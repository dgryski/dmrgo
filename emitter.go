package dmrgo

// Logic for handling mapper and reducer output
// Copyright (c) 2011 Damian Gryski <damian@gryski.com>
// License: GPLv3 or, at your option, any later version

import (
	"bufio"
	"fmt"
	"hash/adler32"
	"os"
)

// Emitter emits key/value pairs
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
		if w != nil {
			w.Flush()
		}
	}
}

func (e *partitionEmitter) Close() {
	for _, w := range e.fds {
		if w != nil {
			w.Close()
		}
	}
}
