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

package storage

import (
	corev1 "k8s.io/api/core/v1"
)

// const fileds
const (
	FieldGCS = "gcs"
	FieldNFS = "nfs"
	FieldFS  = "fs"

	Optional = "optional"
	Expected = "expected"
)

// Spec is a generic cwspec to define a storage destination
// +k8s:deepcopy-gen=true
type Spec struct {
	GCS *GCS                    `json:"gcs,omitempty"`
	NFS *corev1.NFSVolumeSource `json:"nfs,omitempty"`
	FS  *FS                     `json:"fs,omitempty"`
}

// GCS hold gcs related fields
type GCS struct {
	// Bucket name
	Bucket string `json:"bucket,omitempty"`
	// Secret to access bucket
	Secret *corev1.SecretReference `json:"secret,omitempty"`
	// ReadOnly bool
	ReadOnly bool `json:"readonly,omitempty"`
	// Path name
	Path string `json:"path,omitempty"`
}

// FS hold filesystem related fields
type FS struct {
	// Path name
	Path string `json:"path,omitempty"`
}
