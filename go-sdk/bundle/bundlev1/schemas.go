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

// BundleProvider is the single interface a bundle author implements. Construct
// one in main and pass it to bundlev1server.Serve; the runtime calls
// GetBundleVersion to identify the bundle and RegisterDags to load its tasks.
type BundleProvider interface {
	// GetBundleVersion returns upfront information about the bundle name and version without needing to load
	// the full dag and task information, which could be memory intensive.
	GetBundleVersion() BundleInfo

	// RegisterDags declares every dag and task in this bundle on the supplied
	// Registry, for example:
	//
	//	func (m *myBundle) RegisterDags(dagbag bundlev1.Registry) error {
	//		dag := dagbag.AddDag("simple_dag")
	//		dag.AddTask(extract)
	//		dag.AddTask(transform)
	//		return nil
	//	}
	//
	// Register all dags and tasks here. It is called once per process per
	// bundle and cached internally, so you do not have to cache it yourself.
	RegisterDags(Registry) error
}

// BundleInfo identifies a bundle by name and version. It is returned by
// BundleProvider.GetBundleVersion and tells Airflow which bundle to run a task
// with.
type BundleInfo = api.BundleInfo

// TaskInstance identifies the running task by dag_id, run_id, task_id, and
// map_index. It is an alias for the Execution-API type and is what
// XComClient.PushXCom takes.
type TaskInstance = api.TaskInstance

// GetMetadataResponse is the runtime reply that carries a bundle's BundleInfo
// back over the go-plugin transport. It is plumbing between the worker and the
// bundle, not something authors construct.
type GetMetadataResponse struct {
	Bundle BundleInfo
}

// ExecuteTaskWorkload is the runtime payload describing one task to run: its
// TaskInstance, the bundle to load, and an optional log path. The worker
// delivers it to the bundle; authors do not build it themselves.
type ExecuteTaskWorkload struct {
	Token string `json:"token"`

	// TODO: I have a feeling that including fields from API in here might be a problem long term for
	// code-level compatibility

	TI         api.TaskInstance `json:"ti"`
	BundleInfo api.BundleInfo   `json:"bundle_info"`
	LogPath    *string          `json:"log_path,omitempty"`
}
