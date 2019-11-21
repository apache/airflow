/*
Copyright 2018 Google LLC
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reconciler

import (
	"math/rand"
	"time"
)

// Password char space
const (
	PasswordCharNumSpace = "abcdefghijklmnopqrstuvwxyz0123456789"
	PasswordCharSpace    = "abcdefghijklmnopqrstuvwxyz"
)

var (
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// RandomAlphanumericString generates a random password of some fixed length.
func RandomAlphanumericString(strlen int) string {
	result := make([]byte, strlen)
	for i := range result {
		result[i] = PasswordCharNumSpace[random.Intn(len(PasswordCharNumSpace))]
	}
	result[0] = PasswordCharSpace[random.Intn(len(PasswordCharSpace))]
	return string(result[:strlen])
}

// NoUpdate - set lifecycle to noupdate
// Manage by Create Only, Delete.
func NoUpdate(o *Object, v interface{}) {
	o.Lifecycle = LifecycleNoUpdate
}

// DecorateOnly - set lifecycle to decorate
// Manage by update only. Dont create or delete
func DecorateOnly(o *Object, v interface{}) {
	o.Lifecycle = LifecycleDecorate
}

// Merge is used to merge multiple maps into the target map
func (out KVMap) Merge(kvmaps ...KVMap) {
	for _, kvmap := range kvmaps {
		for k, v := range kvmap {
			out[k] = v
		}
	}
}

// ObjectsByType get items from the Object bag
func ObjectsByType(in []Object, t string) []Object {
	var out []Object
	for _, item := range in {
		if item.Type == t {
			out = append(out, item)
		}
	}
	return out
}

// DeleteAt object at index
func DeleteAt(in []Object, index int) []Object {
	var out []Object
	in[index] = in[len(in)-1]
	out = in[:len(in)-1]
	return out
}
