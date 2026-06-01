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
	VariableEnvPrefix   = "AIRFLOW_VAR_"
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

type ConnectionClient interface {
	// GetConnection returns the value of an Airflow Connection.
	//
	// If the conn is not found error will be a wrapped ``ConnectionNotFound``:
	//
	//		conn, err := client.GetConnection(ctx, "my-db")
	//		if errors.Is(err, ConnectinNotFound) {
	//				// Handle not found, set default, return custom error etc
	//		} else {
	//				// Other errors here, such as http network timeouts etc.
	//		}
	GetConnection(ctx context.Context, connID string) (Connection, error)
}

type XComClient interface {
	GetXCom(
		ctx context.Context,
		dagId, runId, taskId string,
		mapIndex *int,
		key string,
		value any,
	) (any, error)
	PushXCom(ctx context.Context, ti api.TaskInstance, key string, value any) error
}

type Client interface {
	VariableClient
	ConnectionClient
	XComClient
}
