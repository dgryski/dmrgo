// Protocols for un/marshaling stream values
// Copyright (c) 2011 Damian Gryski <damian@gryski.com>
// License: GPLv3 or, at your option, any later version

package dmrgo

import (
	"json"
	"reflect"
)

// MRProtocol is a set of routines for marshaling and unmarshaling key/value pairs from the input stream.
// Map Reduce jobs can define their own protocols.
type MRProtocol interface {

        // UnmarshalKV turns strings into their associated values.
        // k should be a pointer to the destination value for the unmarshalled "key"
        // vs should be a pointer to an array for the unmarshalled "values"
	UnmarshalKVs(key string, values []string, k interface{}, vs interface{})

        // MarshalKV turns a key/value pair into a KeyValue 
	MarshalKV(key interface{}, value interface{}) *KeyValue
}

// JSONProtocol parse input/output values as JSON strings
type JSONProtocol struct {
	// empty -- just a type
}

func (p *JSONProtocol) UnmarshalKVs(key string, values []string, k interface{}, vs interface{}) {

	json.Unmarshal([]byte(key), &k)

	vsPtrValue := reflect.ValueOf(vs)
	vsType := reflect.TypeOf(vs).Elem()

	v := reflect.New(vsType).Elem()
	e := reflect.New(vsType.Elem())

	for _, js := range values {
		err := json.Unmarshal([]byte(js), e.Interface())
		if err != nil {
			// skip, for now
			continue
		}
		v = reflect.Append(v, e.Elem())
	}

	vsPtrValue.Elem().Set(v)
}

func (p *JSONProtocol) MarshalKV(key interface{}, value interface{}) *KeyValue {
	k, _ := json.Marshal(key)
	v, _ := json.Marshal(value)
	return &KeyValue{string(k), string(v)}
}
