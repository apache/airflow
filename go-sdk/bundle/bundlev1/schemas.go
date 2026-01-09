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

// Package bundlev1 defines the types and interfaces needed to implement v1 of the Bundle Plugin
package bundlev1

import (
	"github.com/apache/airflow/go-sdk/pkg/api"
)

type BundleProvider interface {
	// GetBundleVersion returns upfront information about the bundle name and version without needing to load
	// the full dag and task information, which could be memory intensive.
	GetBundleVersion() BundleInfo

	// RegisterDags is called to populate the Task functions in the registry in order to execute them.
	//
	// You should populate all dags and tasks in the bundle.
	//
	// This will be called once-per-process-per-bundle and cached internally. You do not have to cache this
	// yourself
	RegisterDags(Registry) error
}

// BundleInfo Schema for telling task which bundle to run with.
type BundleInfo = api.BundleInfo

type TaskInstance = api.TaskInstance

type GetMetadataResponse struct {
	Bundle BundleInfo
}

type ExecuteTaskWorkload struct {
	Token string `json:"token"`

	// TODO: I have a feeling that including fields from API in here might be a problem long term for
	// code-level compatibility

	TI         api.TaskInstance `json:"ti"`
	BundleInfo api.BundleInfo   `json:"bundle_info"`
	LogPath    *string          `json:"log_path,omitempty"`
}
