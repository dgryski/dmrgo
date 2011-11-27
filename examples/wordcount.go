// The standard map/reduce example: counting words
// Copyright (c) 2011 Damian Gryski <damian@gryski.com>
// License: GPLv3 or, at your option, any later version
package main

import (
	"../_obj/dmrgo"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
)

// As example, just to show we can write our own custom protocols
type WordCountProto struct{}

func (p *WordCountProto) UnmarshalKVs(key string, values []string, k interface{}, vs interface{}) {

	kptr := k.(*string)
	*kptr = key

	vsptr := vs.(*[]int)

	v := make([]int, len(values), len(values))

	for i, s := range values {
		v[i], _ = strconv.Atoi(s)
	}

	*vsptr = v
}

func (p *WordCountProto) MarshalKV(key interface{}, value interface{}) *dmrgo.KeyValue {
	ks := key.(string)
	vi := value.(int)

	if vi == 1 {
		return &dmrgo.KeyValue{ks, "1"}
	}

	return &dmrgo.KeyValue{ks, strconv.Itoa(vi)}
}

type MRWordCount struct {
	protocol dmrgo.StreamProtocol // overkill -- we would normally just inline the un/marshal calls

	// mapper variables
	mappedWords int
}

func NewWordCount(proto dmrgo.StreamProtocol) dmrgo.MapReduceJob {

	mr := new(MRWordCount)
	mr.protocol = proto

	return mr
}

func (mr *MRWordCount) Map(key string, value string, emitter dmrgo.Emitter) {

	lower := strings.ToLower(string(value))

	letters := strings.Map(func(rune int) int {
		if rune >= 'a' && rune <= 'z' {
			return rune
		}

		return ' '
	},
		lower)

	trimmed := strings.TrimSpace(letters)
	words := strings.Fields(trimmed)

	for _, word := range words {
		mr.mappedWords++
		kv := mr.protocol.MarshalKV(word, 1)
		emitter.Emit(kv.Key, kv.Value)
	}

}

func (mr *MRWordCount) MapFinal(emitter dmrgo.Emitter) {
	dmrgo.Statusln("finished -- mapped ", mr.mappedWords)
	dmrgo.IncrCounter("Program", "mapped words", mr.mappedWords)
}

func (mr *MRWordCount) Reduce(key string, values []string, emitter dmrgo.Emitter) {

	counts := []int{}
	mr.protocol.UnmarshalKVs(key, values, &key, &counts)

	count := 0
	for _, c := range counts {
		count += c
	}

	kv := mr.protocol.MarshalKV(key, count)
	emitter.Emit(kv.Key, kv.Value)
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {

	var use_proto = flag.String("proto", "wc", "use protocol (json/wc/tsv)")

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var proto dmrgo.StreamProtocol

	if *use_proto == "json" {
		proto = new(dmrgo.JSONProtocol)
	} else if *use_proto == "wc" {
		proto = new(WordCountProto)
	} else if *use_proto == "tsv" {
		proto = new(dmrgo.TSVProtocol)
	} else {
		fmt.Println("unknown proto=", *use_proto)
		os.Exit(1)
	}

	wordCounter := NewWordCount(proto)

	dmrgo.Main(wordCounter)
}
