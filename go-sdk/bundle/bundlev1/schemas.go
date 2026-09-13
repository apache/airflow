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

// Package bundlev1 defines the types and interfaces needed to implement a v1 bundle.
package bundlev1

import (
	"github.com/apache/airflow/go-sdk/sdk"
)

// BundleProvider is the single interface a bundle author implements. Construct
// one in main and pass it to bundlev1server.Serve; the runtime calls
// RegisterDags to discover and load its tasks.
type BundleProvider interface {
	// RegisterDags declares every dag and task in this bundle on the supplied
	// Registry, for example:
	//
	//	func (m *myBundle) RegisterDags(dagbag bundlev1.Registry) error {
	//		dag := dagbag.AddDag("simple_dag")
	//		dag.AddTask(extract)
	//		dag.AddTask(transform, []string{"extract"})
	//		return nil
	//	}
	//
	// Register all dags and tasks here. It is called once per process per
	// bundle and cached internally, so you do not have to cache it yourself.
	RegisterDags(Registry) error
}

// TaskInstance is the task identity exposed by the SDK runtime context and
// accepted by XComClient.PushXCom.
type TaskInstance = sdk.TaskInstance
