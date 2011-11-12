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

	v := []int{}

	for _, s := range values {
		i, _ := strconv.Atoi(s)
		v = append(v, i)
	}

	*vsptr = v
}

func (p *WordCountProto) MarshalKV(key interface{}, value interface{}) *dmrgo.KeyValue {
	ks := key.(string)
	vi := value.(int)
	return &dmrgo.KeyValue{ks, fmt.Sprintf("%d", vi)}
}

type MRWordCount struct {
	protocol dmrgo.MRProtocol // overkill -- we would normally just inline the un/marshal calls

	// mapper variables
	mappedWords int
}

func NewWordCount(proto dmrgo.MRProtocol) dmrgo.MapReduceJob {

	mr := new(MRWordCount)
	mr.protocol = proto

	return mr
}

func (mr *MRWordCount) Map(key string, value string, emit dmrgo.Emitter) {

	words := strings.Fields(strings.TrimSpace(strings.ToLower(value)))
	for _, word := range words {
		mr.mappedWords++
		emit.Emit(word, "1")
		//		emit.Emit(*mr.protocol.MarshalKV(word, 1))
	}

}

func (mr *MRWordCount) MapFinal(emit dmrgo.Emitter) {
	dmrgo.Statusln("finished -- mapped ", mr.mappedWords)
	dmrgo.IncrCounter("Program", "mapped words", mr.mappedWords)
}

func (mr *MRWordCount) Reduce(key string, values []string, emit dmrgo.Emitter) {

	counts := []int{}
	mr.protocol.UnmarshalKVs(key, values, &key, &counts)

	count := 0
	for _, c := range counts {
		count += c
	}

	emit.Emit(key, fmt.Sprintf("%d", count))
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {

	var use_proto = flag.String("proto", "json", "use protocol (json/wc)")

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var proto dmrgo.MRProtocol

	if *use_proto == "json" {
		proto = new(dmrgo.JSONProtocol)
	} else if *use_proto == "wc" {
		proto = new(WordCountProto)
	} else {
		fmt.Println("unknown proto=", use_proto)
		os.Exit(1)
	}

	wordCounter := NewWordCount(proto)

	dmrgo.Main(wordCounter)
}
