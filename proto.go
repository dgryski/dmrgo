// Protocols for un/marshaling stream values
// Copyright (c) 2011 Damian Gryski <damian@gryski.com>
// License: GPLv3 or, at your option, any later version

package dmrgo

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// StreamProtocol is a set of routines for marshaling and unmarshaling key/value pairs from the input stream.
// Map Reduce jobs can define their own protocols.
type StreamProtocol interface {

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

	v := reflect.MakeSlice(vsType, len(values), len(values))

	for i, js := range values {
		e := v.Index(i)
		err := json.Unmarshal([]byte(js), e.Addr().Interface())
		if err != nil {
			// skip, for now
			continue
		}
	}

	vsPtrValue.Elem().Set(v)
}

func (p *JSONProtocol) MarshalKV(key interface{}, value interface{}) *KeyValue {
	k, _ := json.Marshal(key)
	v, _ := json.Marshal(value)
	return &KeyValue{string(k), string(v)}
}

type TSVProtocol struct {
	// empty -- just a type
}

func (p *TSVProtocol) MarshalKV(key interface{}, value interface{}) *KeyValue {

	kVal := reflect.ValueOf(key)
	k := primitiveToString(kVal)

	var vs []string

	vType := reflect.TypeOf(value)
	vVal := reflect.ValueOf(value)

	if vType.Kind() == reflect.Struct {
		vs = make([]string, vType.NumField(), vType.NumField())
		for i := 0; i < vType.NumField(); i++ {
			field := vVal.Field(i)
			vs[i] = primitiveToString(field)
		}
	} else if isPrimitive(vType.Kind()) {
		vs = append(vs, primitiveToString(vVal))
	} else if vType.Kind() == reflect.Array || vType.Kind() == reflect.Slice {
		vs = make([]string, vVal.Len(), vVal.Len())
		for i := 0; i < vVal.Len(); i++ {
			field := vVal.Index(i)
			// arrays/slices must be of primitives
			vs[i] = primitiveToString(field)
		}
	}

	vals := strings.Join(vs, "\t")

	return &KeyValue{k, vals}
}

func (p *TSVProtocol) UnmarshalKVs(key string, values []string, k interface{}, vs interface{}) {

	fmt.Sscan(key, &k)

	vsPtrValue := reflect.ValueOf(vs)
	vsType := reflect.TypeOf(vs).Elem()
	vType := vsType.Elem()

	v := reflect.MakeSlice(vsType, len(values), len(values))

	for vi, s := range values {
		vs := strings.Split(s, "\t")

		// create our new element
		e := v.Index(vi)

		// figure out what kind we need to unpack our data into
		if vType.Kind() == reflect.Struct {
			for i := 0; i < vType.NumField(); i++ {
				_, err := fmt.Sscan(vs[i], e.Field(i).Addr().Interface())
				if err != nil {
					continue // skip
				}
			}
		} else if vType.Kind() == reflect.Array {
			for i := 0; i < vType.Len(); i++ {
				_, err := fmt.Sscan(vs[i], e.Index(i).Addr().Interface())
				if err != nil {
					continue // skip
				}
			}
		} else if isPrimitive(vType.Kind()) {
			fmt.Sscan(vs[0], e.Addr().Interface())
		}
	}

	vsPtrValue.Elem().Set(v)
}

func isPrimitive(k reflect.Kind) bool {

	switch k {
	case reflect.Bool:
		fallthrough
	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.String:
		return true
	}

	return false
}

func primitiveToString(v reflect.Value) string {

	switch v.Kind() {

	case reflect.Bool:
		if v.Bool() {
			return "1"
		}
		return "0"
	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)

	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)

	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'g', 5, 64)
	case reflect.String:
		return v.String()
	}

	return "(unknown type " + string(v.Kind()) + ")"
}
