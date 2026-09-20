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
	"math"
	"time"
)

const (
	// VariableEnvPrefix is the environment-variable prefix used as a local
	// fallback for Variable lookups. GetVariable first checks the process
	// environment for VariableEnvPrefix plus the uppercased key (so key
	// "my_var" is read from AIRFLOW_VAR_MY_VAR) before asking Airflow,
	// mirroring the Python SDK and making local development and tests easy.
	VariableEnvPrefix = "AIRFLOW_VAR_"

	// ConnectionEnvPrefix is the matching prefix for Connections. The
	// connection env fallback is not wired up yet, so it is currently unused.
	ConnectionEnvPrefix = "AIRFLOW_CONN_"

	// XComReturnValueKey is the key Airflow uses for a task's returned value.
	XComReturnValueKey = "return_value"
)

// NeverExpire marks a task state key as exempt from expiry. Pass it as the
// retention argument of the SetTaskStateWithRetention method of
// [TaskStateStoreClient] and the key is skipped by garbage collection, so it
// lives until the task deletes it or the Dag run itself is removed. It
// overrides the deployment's “[state_store] default_retention_days“ setting,
// and is the Go spelling of the Python SDK's “airflow.sdk.NEVER_EXPIRE“.
const NeverExpire = time.Duration(math.MaxInt64)

// VariableClient reads, writes, and deletes Airflow Variables.
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
	// It first looks in the process environment for the appropriately named
	// variable and, if absent, asks Airflow through the coordinator.
	//
	// If the variable is not found error will be a wrapped ``VariableNotFound``:
	//
	//		val, err := client.GetVariable(ctx, "my-var")
	//		if errors.Is(err, VariableNotFound) {
	//				// Handle not found, set default, return custom error etc
	//		} else {
	//				// Other errors here, such as transport timeouts etc.
	//		}
	GetVariable(ctx context.Context, key string) (string, error)

	// UnmarshalJSONVariable fetches a variable and unmarshals its value into
	// pointer via json.Unmarshal. Use this when the variable was stored as a
	// JSON object, array, or number; for plain string variables call
	// GetVariable directly.
	//
	// pointer must be a non-nil pointer, as required by encoding/json.
	UnmarshalJSONVariable(ctx context.Context, key string, pointer any) error

	// SetVariable stores value under key, creating the Variable or replacing
	// an existing one. An empty description is sent as null, which clears any
	// description the Variable already had.
	//
	// The value is stored as-is: encode structured data (for example with
	// json.Marshal) before storing it. A value supplied by a secrets backend
	// (for example an AIRFLOW_VAR_<KEY> environment variable) still takes
	// precedence over the stored value when the Variable is read back.
	SetVariable(ctx context.Context, key, value, description string) error

	// DeleteVariable removes the Variable stored under key.
	DeleteVariable(ctx context.Context, key string) error
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
	//				// Other errors here, such as transport timeouts etc.
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
	PushXCom(ctx context.Context, ti TaskInstance, key string, value any) error
}

// TaskStateStoreClient reads and writes the task state store: a persistent
// key/value store private to one task instance, and the mechanism behind
// durable execution.
//
// The store is scoped to dag_id, run_id, task_id, and map_index. It
// deliberately does not include try_number, so a value written by one attempt
// is still readable by the next one: a task that records its progress can pick
// up where it left off after a worker crash or a retry, instead of redoing
// work. The Execution API confines every call to the task instance the caller
// is running as, so there is no way to address another task's store — pass
// results between tasks with XCom instead.
type TaskStateStoreClient interface {
	// GetTaskState returns the value stored under key for this task instance.
	//
	// If the key is not found error will be a wrapped ``TaskStateNotFound``:
	//
	//		val, err := client.GetTaskState(ctx, "checkpoint")
	//		if errors.Is(err, TaskStateNotFound) {
	//				// Handle not found, set default, return custom error etc
	//		} else {
	//				// Other errors here, such as transport timeouts etc.
	//		}
	GetTaskState(ctx context.Context, key string) (any, error)

	// UnmarshalJSONTaskState fetches a task state value and unmarshals it into
	// pointer via json.Unmarshal. Use this when the value was stored as a JSON
	// object or array; for scalars such as strings, numbers, and booleans call
	// GetTaskState directly.
	//
	// pointer must be a non-nil pointer, as required by encoding/json.
	UnmarshalJSONTaskState(ctx context.Context, key string, pointer any) error

	// SetTaskState stores value under key, creating the entry or replacing an
	// existing one. The key expires according to the deployment's
	// “[state_store] default_retention_days“ setting; use
	// SetTaskStateWithRetention to choose the lifetime yourself. A deployment
	// that sets it to something unusable fails the write rather than falling
	// back to a different lifetime.
	//
	// value must not be nil and must be JSON-representable: a string, number,
	// bool, slice, map, or a struct (which is stored as an object). A value the
	// store cannot hold is rejected before it is sent — notably a time.Time,
	// which has no JSON spelling: store value.Format(time.RFC3339) instead.
	SetTaskState(ctx context.Context, key string, value any) error

	// SetTaskStateWithRetention stores value under key like SetTaskState, but
	// keeps the key for retention instead of the deployment default.
	//
	// retention must be positive, or [NeverExpire] to exempt the key from
	// expiry altogether. A zero or negative retention is rejected rather than
	// given a meaning of its own: to follow the deployment default call
	// SetTaskState, and to drop a key call DeleteTaskState.
	SetTaskStateWithRetention(
		ctx context.Context,
		key string,
		value any,
		retention time.Duration,
	) error

	// DeleteTaskState removes the value stored under key. Deleting a key that
	// does not exist is not an error.
	DeleteTaskState(ctx context.Context, key string) error

	// ClearTaskState removes every key stored for this task instance.
	ClearTaskState(ctx context.Context) error
}

// Client is the full task-facing API: read/write Variables, read Connections,
// read/write XCom, and read/write the task state store. A task gets one from
// its airflow.Context by calling actx.Client(). A helper that needs only one
// capability can take the narrower VariableClient, ConnectionClient,
// XComClient, or TaskStateStoreClient instead.
type Client interface {
	VariableClient
	ConnectionClient
	XComClient
	TaskStateStoreClient
}
