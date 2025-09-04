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

/*
Package sdk provides access to the Airflow objects (Variables, Connection, XCom etc) during run time for tasks.
*/
package sdk

import (
	"context"
)

const (
	VariableEnvPrefix   = "AIRFLOW_VAR_"
	ConnectionEnvPrefix = "AIRFLOW_CONN_"
)

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
	UnmarshalJSONVariable(ctx context.Context, key string, pointer any) error
}

type Client interface {
	VariableClient
}
