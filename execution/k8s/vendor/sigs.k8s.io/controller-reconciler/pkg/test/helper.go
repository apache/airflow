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

package test

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"os"
	"path/filepath"
	"strconv"
)

// OpenFile - open file
func OpenFile(path string) (*os.File, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// AdjustCPU - increase/decrease cpu
func AdjustCPU(r *corev1.ResourceRequirements, by int64) {
	newValue := r.Requests.Cpu().MilliValue() + by
	r.Requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(newValue, resource.BinarySI)
}

// AddMemoryGB - increase Memory
func AddMemoryGB(r *corev1.ResourceRequirements, req int, limit int) {
	mem := r.Requests[corev1.ResourceMemory]
	mmem := &mem
	add, _ := resource.ParseQuantity(strconv.Itoa(req) + "Gi")
	mmem.Add(add)
	r.Requests[corev1.ResourceMemory] = mem

	mem = r.Limits[corev1.ResourceMemory]
	mmem = &mem
	add, _ = resource.ParseQuantity(strconv.Itoa(limit) + "Gi")
	mmem.Add(add)
	r.Limits[corev1.ResourceMemory] = mem
}

//= corev1.ResourceList{
//corev1.ResourceCPU:    *resource.NewMilliQuantity(newValue, resource.BinarySI),
//corev1.ResourceMemory: *ng.Resources.Requests.Memory(),
//}
