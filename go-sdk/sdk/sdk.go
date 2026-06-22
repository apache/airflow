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
	"context"

	"github.com/apache/airflow/go-sdk/pkg/api"
)

const (
	// VariableEnvPrefix is the environment-variable prefix used as a local
	// fallback for Variable lookups. GetVariable first checks the process
	// environment for VariableEnvPrefix plus the uppercased key (so key
	// "my_var" is read from AIRFLOW_VAR_MY_VAR) before asking the API server,
	// mirroring the Python SDK and making local development and tests easy.
	VariableEnvPrefix = "AIRFLOW_VAR_"

	// ConnectionEnvPrefix is the matching prefix for Connections. The
	// connection env fallback is not wired up yet, so it is currently unused.
	ConnectionEnvPrefix = "AIRFLOW_CONN_"
)

// VariableClient reads Airflow Variables.
//
// Go has no function overloading, so the "give me the raw string" and
// "give me a decoded struct" cases are split into two methods rather
// than one polymorphic call: GetVariable returns the raw string,
// UnmarshalJSONVariable decodes a JSON-encoded variable into a
// caller-supplied pointer. This mirrors the std-lib split between
// os.LookupEnv and json.Unmarshal — each method has one job, and the
// caller picks based on how the variable was stored.
type VariableClient interface {
	// GetVariable returns the value of an Airflow Variable.
	//
	// It will first look in the os.environ for the appropriately named variable, and if not found there will
	// fallback to asking the API server
	//
	// If the variable is not found error will be a wrapped ``VariableNotFound``:
	//
	//		val, err := client.GetVariable(ctx, "my-var")
	//		if errors.Is(err, VariableNotFound) {
	//				// Handle not found, set default, return custom error etc
	//		} else {
	//				// Other errors here, such as http network timeouts etc.
	//		}
	GetVariable(ctx context.Context, key string) (string, error)

	// UnmarshalJSONVariable fetches a variable and unmarshals its value into
	// pointer via json.Unmarshal. Use this when the variable was stored as a
	// JSON object, array, or number; for plain string variables call
	// GetVariable directly.
	//
	// pointer must be a non-nil pointer, as required by encoding/json.
	UnmarshalJSONVariable(ctx context.Context, key string, pointer any) error
}

// ConnectionClient reads Airflow Connections.
type ConnectionClient interface {
	// GetConnection returns the value of an Airflow Connection.
	//
	// If the conn is not found error will be a wrapped ``ConnectionNotFound``:
	//
	//		conn, err := client.GetConnection(ctx, "my-db")
	//		if errors.Is(err, ConnectionNotFound) {
	//				// Handle not found, set default, return custom error etc
	//		} else {
	//				// Other errors here, such as http network timeouts etc.
	//		}
	GetConnection(ctx context.Context, connID string) (Connection, error)
}

// XComClient reads and writes XCom values. Most tasks never need this: to
// publish a result, return a value from the task function and the runtime
// pushes it as the return-value XCom. Reach for these methods only to read
// another task's XCom, or to push under a custom key.
type XComClient interface {
	// GetXCom returns the value stored under key by the task identified by
	// dagId/runId/taskId. For a mapped task instance pass its mapIndex,
	// otherwise pass nil. If no value exists the error wraps XComNotFound.
	//
	// value is reserved for future typed decoding and is currently ignored; the
	// stored value is returned as the first result instead.
	GetXCom(
		ctx context.Context,
		dagId, runId, taskId string,
		mapIndex *int,
		key string,
		value any,
	) (any, error)

	// PushXCom stores value under key for the given task instance ti.
	PushXCom(ctx context.Context, ti api.TaskInstance, key string, value any) error
}

// Client is the full task-facing API: read Variables and Connections, and
// read/write XCom. A task that declares an sdk.Client parameter is handed one
// by the runtime. If a task needs only one capability, ask for the narrower
// VariableClient, ConnectionClient, or XComClient instead.
type Client interface {
	VariableClient
	ConnectionClient
	XComClient
}
