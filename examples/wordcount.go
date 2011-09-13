package main

import (
	"fmt"
	"strings"
	"../_obj/dmrgo"
	"strconv"
	"flag"
	"os"
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

// word count map/reduce logic here
// overkill for such a simple example -- we would probably actually inline the conversion routines

type MRWordCount struct {
	protocol dmrgo.MRProtocol

	// mapper variables
	mappedWords int
}

func NewWordCount(proto dmrgo.MRProtocol) dmrgo.MapReduceJob {

	mr := new(MRWordCount)
	mr.protocol = proto

	return mr
}

func (mr *MRWordCount) Map(key string, value string) []*dmrgo.KeyValue {

	words := strings.Split(strings.TrimSpace(value), " ")
	kvs := make([]*dmrgo.KeyValue, len(words))
	for i, word := range words {

		mr.mappedWords++
		if mr.mappedWords%1000 == 0 {
			dmrgo.Statusln("mapped ", mr.mappedWords)
		}

		kvs[i] = mr.protocol.MarshalKV(word, 1)
	}

	dmrgo.IncrCounter("Program", "mapped lines", 1)

	return kvs
}

func (mr *MRWordCount) MapFinal() []*dmrgo.KeyValue {
	dmrgo.Statusln("finished -- mapped ", mr.mappedWords)
	return []*dmrgo.KeyValue{}
}

func (mr *MRWordCount) Reduce(key string, values []string) []*dmrgo.KeyValue {

	counts := []int{}
	mr.protocol.UnmarshalKVs(key, values, &key, &counts)

	count := 0
	for _, c := range counts {
		count += c
	}

	return []*dmrgo.KeyValue{mr.protocol.MarshalKV(key, count)}
}

// A lot of this code is boiler plate code and should be extracted to the library
func main() {

	var do_map = flag.Bool("mapper", false, "run mapper code on stdin")
	var do_reduce = flag.Bool("reducer", false, "run reducer on stdin")
	var use_proto = flag.String("proto", "json", "use protocol (json/wc)")

	flag.Parse()

	if *do_map && *do_reduce {
		fmt.Println("can either map or reduce, not both")
		os.Exit(1)
	}

	if !*do_map && !*do_reduce {
		fmt.Println("neither map not reduce called")
		os.Exit(1)
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

	if *do_map {
		dmrgo.RunMapper(wordCounter, os.Stdin, os.Stdout)
	}

	if *do_reduce {
		dmrgo.RunReducer(wordCounter, os.Stdin, os.Stdout)
	}

	os.Exit(0)
}
