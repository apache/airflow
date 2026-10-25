// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sdk

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
)

// UnmarshalConf reads [DagRun.Conf] into target, which must be a non-nil
// pointer, usually to a struct with one field per conf key:
//
//	type etlConf struct {
//		TargetTable string
//		Limit       int
//		Since       time.Time
//	}
//
//	etl := etlConf{Limit: 100}
//	if err := actx.DagRun().UnmarshalConf(&etl); err != nil {
//		return err
//	}
//
// For a run triggered with {"target_table": "sales", "since": "2024-01-31T00:00:00Z"},
// etl.TargetTable is "sales", etl.Since is that date, and etl.Limit stays 100.
//
// Each key goes to the field whose name matches it when case and underscores
// are ignored, so target_table reaches TargetTable. A `json:` tag renames the
// field, and the tag name matches the same way. If one key matches a field
// name exactly, that key wins over keys that match it only once case and
// underscores are ignored. Objects nested in the conf follow the same rules,
// including objects in slices and objects that are map values. The keys of a
// map field stay as written, because they are data rather than field names.
//
// Keys that match no field are ignored, because other tasks in the run may
// read them. A field with no matching key keeps the value target already
// holds, so set the defaults on target before the call, as Limit does above.
// The conf carries only the values the run was triggered with: it does not
// carry the defaults of the Dag's params, so in a scheduled run every field
// keeps the value it came in with.
//
// The `arg:` tag that binds a TaskFlow argument has no effect here. Read a key
// that no field name can match, such as target-table, from [DagRun.Conf], or
// name it in a `json:` tag.
//
// It returns an error when a value does not fit the type of its field, when
// two keys match one field only once case and underscores are ignored, or when
// two fields of a struct differ only in case or underscores.
func (d DagRun) UnmarshalConf(target any) error {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf(
			"sdk.DagRun.UnmarshalConf: target must be a non-nil pointer, got %T",
			target,
		)
	}
	matched, err := matchConfValue(map[string]any(d.Conf), rv.Type().Elem())
	if err != nil {
		return err
	}
	blob, err := json.Marshal(matched)
	if err != nil {
		return fmt.Errorf("sdk.DagRun.UnmarshalConf: cannot encode the Dag run conf: %w", err)
	}
	if err := json.NewDecoder(bytes.NewReader(blob)).Decode(target); err != nil {
		return fmt.Errorf(
			"sdk.DagRun.UnmarshalConf: cannot read the Dag run conf into %s: %w",
			rv.Type().Elem(),
			err,
		)
	}
	return nil
}

// matchConfValue renames the keys of every conf object that becomes a struct
// when the conf is decoded into target. encoding/json matches a key to a field
// only when the two differ at most in case, so renaming the keys to the exact
// field names is what lets target_table reach TargetTable.
//
// The walk follows target rather than the conf, so it reaches objects nested
// in slices, arrays and map values, and stops at a type that decodes itself.
func matchConfValue(value any, target reflect.Type) (any, error) {
	for target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	// A type such as time.Time decodes from a value of its own shape, and its
	// Go fields are not conf keys.
	if implementsConfUnmarshaler(target) {
		return value, nil
	}

	switch target.Kind() {
	case reflect.Struct:
		if obj, ok := value.(map[string]any); ok {
			return matchConfFields(obj, target)
		}
	case reflect.Map:
		if obj, ok := value.(map[string]any); ok {
			matched := make(map[string]any, len(obj))
			for key, elem := range obj {
				elem, err := matchConfValue(elem, target.Elem())
				if err != nil {
					return nil, err
				}
				matched[key] = elem
			}
			return matched, nil
		}
	case reflect.Slice, reflect.Array:
		if list, ok := value.([]any); ok {
			matched := make([]any, len(list))
			for i, elem := range list {
				elem, err := matchConfValue(elem, target.Elem())
				if err != nil {
					return nil, err
				}
				matched[i] = elem
			}
			return matched, nil
		}
	}
	// A value whose shape does not fit the field, such as a string for a
	// struct, passes through so that the decoder reports the mismatch.
	return value, nil
}

// matchConfFields renames each key of obj to the name of the target field the
// key matches, and drops a key that matches no field. Keys are visited in
// sorted order so that an error names the same pair of keys on every run.
func matchConfFields(obj map[string]any, target reflect.Type) (map[string]any, error) {
	fields, err := confFieldsOf(target)
	if err != nil {
		return nil, err
	}

	matched := make(map[string]any, len(obj))
	keyOfField := make(map[string]string, len(obj))
	for _, key := range slices.Sorted(maps.Keys(obj)) {
		field, ok := fields.byName[key]
		if !ok {
			// A key that folds to a field claims that field only when no other
			// key names it exactly.
			folded, found := fields.byFold[foldConfName(key)]
			if !found {
				continue
			}
			if _, exact := obj[folded.name]; exact {
				continue
			}
			field = folded
		}
		if other, dup := keyOfField[field.name]; dup {
			return nil, fmt.Errorf(
				"sdk.DagRun.UnmarshalConf: Dag run conf keys %q and %q both match field %s when case and "+
					"underscores are ignored, so the SDK cannot tell which one to use; name the field or "+
					"its json tag exactly as the key it should take",
				other,
				key,
				field.goName,
			)
		}
		keyOfField[field.name] = key

		value, err := matchConfValue(obj[key], field.typ)
		if err != nil {
			return nil, err
		}
		matched[field.name] = value
	}
	return matched, nil
}

// confField is one field a conf key can reach.
type confField struct {
	// name is the key the decoder matches the field by: the `json:` tag name
	// when the field has one, and the Go field name otherwise.
	name string
	// goName is the field as an error names it, qualified with the struct that
	// declares it, which is an embedded struct for a promoted field.
	goName string
	typ    reflect.Type
}

type confFields struct {
	byName map[string]confField
	byFold map[string]confField
	err    error
}

// Indexing a struct costs far more than renaming the keys of one object of
// that struct, so a conf holding a list of many objects would otherwise spend
// most of its time indexing the same struct again.
var confFieldCache sync.Map // reflect.Type -> *confFields

func confFieldsOf(target reflect.Type) (*confFields, error) {
	if cached, ok := confFieldCache.Load(target); ok {
		fields := cached.(*confFields)
		return fields, fields.err
	}
	fields := indexConfFields(target)
	confFieldCache.Store(target, fields)
	return fields, fields.err
}

func indexConfFields(target reflect.Type) *confFields {
	fields := &confFields{
		byName: map[string]confField{},
		byFold: map[string]confField{},
	}
	// Breadth-first over embedded structs, because a field of the outer struct
	// shadows the field of the same name it promotes, as encoding/json does.
	// A struct can embed a pointer to itself, so each one is visited once.
	visited := map[reflect.Type]bool{target: true}
	for level := []reflect.Type{target}; len(level) > 0; {
		var embedded []reflect.Type
		for _, structType := range level {
			for _, inner := range collectConfFields(structType, fields) {
				if !visited[inner] {
					visited[inner] = true
					embedded = append(embedded, inner)
				}
			}
		}
		if fields.err != nil {
			return fields
		}
		level = embedded
	}
	return fields
}

// collectConfFields adds the fields one struct declares and returns the
// structs it embeds, whose fields belong to the next level.
func collectConfFields(structType reflect.Type, fields *confFields) []reflect.Type {
	var embedded []reflect.Type
	for i := range structType.NumField() {
		f := structType.Field(i)
		name, tagged := confFieldName(f)
		if f.Anonymous && !tagged {
			if inner := confStructType(f.Type); inner != nil {
				embedded = append(embedded, inner)
				continue
			}
		}
		if !f.IsExported() || name == "" {
			continue
		}
		if _, shadowed := fields.byName[name]; shadowed {
			continue
		}
		field := confField{
			name:   name,
			goName: confTypeName(structType) + "." + f.Name,
			typ:    f.Type,
		}
		fields.byName[name] = field
		folded := foldConfName(name)
		if clash, dup := fields.byFold[folded]; dup {
			fields.err = fmt.Errorf(
				"sdk.DagRun.UnmarshalConf: fields %s and %s differ only in case or underscores, so the SDK "+
					"cannot tell which one a Dag run conf key should go to; rename one of them",
				clash.goName,
				field.goName,
			)
			return nil
		}
		fields.byFold[folded] = field
	}
	return embedded
}

// confFieldName returns the key the decoder matches a field by, and whether a
// `json:` tag names it. An empty name means the field takes no key at all,
// which is what `json:"-"` asks for.
func confFieldName(f reflect.StructField) (name string, tagged bool) {
	tag, ok := f.Tag.Lookup("json")
	if !ok {
		return f.Name, false
	}
	tagName, _, _ := strings.Cut(tag, ",")
	switch tagName {
	case "-":
		// `json:"-,"` names the field "-"; only a bare `json:"-"` drops it.
		if tag == "-" {
			return "", false
		}
		return "-", true
	case "":
		return f.Name, false
	}
	return tagName, true
}

// confStructType returns the struct an embedded field promotes fields from, or
// nil when the field promotes no conf key.
func confStructType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || implementsConfUnmarshaler(t) {
		return nil
	}
	return t
}

func foldConfName(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "_", ""))
}

func confTypeName(t reflect.Type) string {
	if t.Name() == "" {
		return t.String()
	}
	return t.Name()
}

func implementsConfUnmarshaler(t reflect.Type) bool {
	for _, candidate := range []reflect.Type{t, reflect.PointerTo(t)} {
		if candidate.Implements(jsonUnmarshalerType) || candidate.Implements(textUnmarshalerType) {
			return true
		}
	}
	return false
}

var (
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
)
