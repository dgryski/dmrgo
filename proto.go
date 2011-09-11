package dmrgo

import (
	"json"
	"reflect"
)

type MRProtocol interface {
	UnmarshalKVs(key string, values []string, k interface{}, vs interface{})
	MarshalKV(key interface{}, value interface{}) *KeyValue
}

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
