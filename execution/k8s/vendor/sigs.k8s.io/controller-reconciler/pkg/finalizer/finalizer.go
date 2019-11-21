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

package finalizer

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// const fileds
const (
	Cleanup = "sigapps.k8s.io/cleanup"
)

// Exists adds the finalizer string
func Exists(o metav1.Object, new string) bool {
	exists := false
	existing := o.GetFinalizers()
	for _, f := range existing {
		if f == new {
			exists = true
			break
		}
	}

	return exists
}

// Add adds the finalizer string
func Add(o metav1.Object, new string) {
	exists := false
	existing := o.GetFinalizers()
	for _, f := range existing {
		if f == new {
			exists = true
			break
		}
	}

	if !exists {
		existing = append(existing, new)
		o.SetFinalizers(existing)
	}
}

// Remove removes the finalizer string
func Remove(o metav1.Object, new string) {
	foundat := -1
	existing := o.GetFinalizers()
	for i, f := range existing {
		if f == new {
			foundat = i
			break
		}
	}
	if foundat != -1 {
		existing[foundat] = existing[len(existing)-1]
		o.SetFinalizers(existing[:len(existing)-1])
	}
}

// EnsureStandard adds standard finalizers
func EnsureStandard(o metav1.Object) {
	Add(o, Cleanup)
}

// RemoveStandard removes standard finalizers
func RemoveStandard(o metav1.Object) {
	Remove(o, Cleanup)
}
