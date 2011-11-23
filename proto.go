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
		for i := 0; i < vType.NumField(); i++ {
			field := vVal.Field(i)
			v := primitiveToString(field)
			vs = append(vs, v)
		}
	} else if vType.Kind() == reflect.String {
		vs = append(vs, vVal.String())
	} else if vType.Kind() == reflect.Array || vType.Kind() == reflect.Slice {
		for i := 0; i < vVal.Len(); i++ {
			field := vVal.Index(i)
			v := fmt.Sprint(field.Interface())
			vs = append(vs, v)
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

	v := reflect.New(vsType).Elem()

	for _, s := range values {
		vs := strings.Split(s, "\t")

		// create our new element
		e := reflect.New(vsType.Elem())

		// figure out what kind we need to unpack our data into
		if vType.Kind() == reflect.Struct {
			for i := 0; i < vType.NumField(); i++ {
				_, err := fmt.Sscan(vs[i], e.Elem().Field(i).Addr().Interface())
				if err != nil {
					continue // skip
				}
			}
		} else if vType.Kind() == reflect.Array {
			for i := 0; i < vType.Len(); i++ {
				_, err := fmt.Sscan(vs[i], e.Elem().Index(i).Addr().Interface())
				if err != nil {
					continue // skip
				}
			}
		}

		// add it to our list
		v = reflect.Append(v, e.Elem())
	}

	vsPtrValue.Elem().Set(v)
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
		return strconv.Itoa64(v.Int())

	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		return strconv.Uitoa64(v.Uint())

	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		return strconv.Ftoa64(v.Float(), 'g', 5)
	case reflect.String:
		return v.String()
	}

	return "(unknown)"
}
