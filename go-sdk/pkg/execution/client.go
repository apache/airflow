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

package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/sdk"
)

// Supervisor-side error codes carried in the "error" field of an
// ErrorResponse frame. The Python source of truth is
// airflow.sdk.exceptions.ErrorType.
const (
	errCodeVariableNotFound   = "VARIABLE_NOT_FOUND"
	errCodeConnectionNotFound = "CONNECTION_NOT_FOUND"
	errCodeXComNotFound       = "XCOM_NOT_FOUND"
)

// translateApiError converts a supervisor *ApiError whose Err field matches
// code into a sentinel-wrapped error (matching the formatting the HTTP-backed
// sdk.client uses for the same condition). Any other error - including a
// *ApiError with a different code - is returned unchanged so callers can keep
// distinguishing transport / server errors from "thing not found".
func translateApiError(err error, code string, sentinel error, key string) error {
	if err == nil {
		return nil
	}
	var apiErr *ApiError
	if errors.As(err, &apiErr) && apiErr.Err == code {
		return fmt.Errorf("%w: %q", sentinel, key)
	}
	return err
}

// CoordinatorClient implements sdk.Client by communicating with the Airflow supervisor
// over the comm socket using msgpack-framed IPC instead of HTTP.
type CoordinatorClient struct {
	comm *CoordinatorComm
}

var _ sdk.Client = (*CoordinatorClient)(nil)

// NewCoordinatorClient creates a new client backed by the comm socket.
func NewCoordinatorClient(comm *CoordinatorComm) *CoordinatorClient {
	return &CoordinatorClient{
		comm: comm,
	}
}

// GetVariable requests a variable value from the supervisor.
func (c *CoordinatorClient) GetVariable(ctx context.Context, key string) (string, error) {
	// TODO: this duplicates variableFromEnv in sdk/client.go. The env-first
	// precedence is part of the SDK contract, so both clients should share a
	// single helper (e.g. an exported sdk.VariableFromEnv) instead of two
	// independent copies that can drift.
	if env, ok := os.LookupEnv(sdk.VariableEnvPrefix + strings.ToUpper(key)); ok {
		return env, nil
	}

	resp, err := c.comm.Communicate(
		ctx,
		genmodels.GetVariable{Key: key},
	)
	if err != nil {
		return "", translateApiError(err, errCodeVariableNotFound, sdk.VariableNotFound, key)
	}

	var result genmodels.VariableResult
	if err := decodeBody(resp, &result); err != nil {
		return "", fmt.Errorf("decoding variable result: %w", err)
	}

	if result.Value == nil {
		return "", fmt.Errorf("%w: %q", sdk.VariableNotFound, key)
	}

	// TODO: register secret-named variables with a SecretsMasker so the
	// returned value is automatically redacted from subsequent task logs,
	// matching Python's airflow.models.variable.Variable.get behaviour.
	// Pairs with the "TODO: mask secrets here" hook in
	// pkg/worker/runner.go's task log handler — both halves are needed
	// before secret masking actually works end-to-end.

	switch v := result.Value.(type) {
	case string:
		return v, nil
	default:
		// Airflow Variables are stored as strings, but the supervisor
		// decodes msgpack into native Go types — a supervisor that
		// returns a list/map/number means the caller stored JSON.
		// Re-encode it so UnmarshalJSONVariable still works uniformly
		// across HTTP and coordinator modes.
		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("marshaling variable value: %w", err)
		}
		return string(b), nil
	}
}

// UnmarshalJSONVariable gets a variable and unmarshals its JSON value.
func (c *CoordinatorClient) UnmarshalJSONVariable(
	ctx context.Context,
	key string,
	pointer any,
) error {
	val, err := c.GetVariable(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), pointer)
}

// GetConnection requests a connection from the supervisor.
func (c *CoordinatorClient) GetConnection(
	ctx context.Context,
	connID string,
) (sdk.Connection, error) {
	resp, err := c.comm.Communicate(
		ctx,
		genmodels.GetConnection{ConnID: connID},
	)
	if err != nil {
		return sdk.Connection{}, translateApiError(
			err, errCodeConnectionNotFound, sdk.ConnectionNotFound, connID,
		)
	}

	var result genmodels.ConnectionResult
	if err := decodeBody(resp, &result); err != nil {
		return sdk.Connection{}, fmt.Errorf("decoding connection result: %w", err)
	}

	conn := sdk.Connection{
		ID:   result.ConnID,
		Type: result.ConnType,
		Host: ifaceString(result.Host),
		Port: ifaceInt(result.Port, 0),
		Path: ifaceString(result.Schema),
	}

	// Preserve the null-vs-empty distinction on credentials so an explicitly
	// empty credential (distinct from "no credential set") survives the
	// coordinator hop and reaches sdk.Connection's URI-building code the same
	// way it does in the HTTP-backed SDK. The supervisor schema types these as
	// nullable strings, decoded here from the generated `any` fields.
	conn.Login = ifaceStringPtr(result.Login)
	conn.Password = ifaceStringPtr(result.Password)
	if extra := ifaceString(result.Extra); extra != "" {
		conn.Extra = map[string]any{}
		if err := json.Unmarshal([]byte(extra), &conn.Extra); err != nil {
			return conn, fmt.Errorf("parsing connection extra: %w", err)
		}
	}

	// TODO: register conn.Password and sensitive-keyed entries of conn.Extra
	// with a SecretsMasker so they are auto-redacted from subsequent task
	// logs, matching Python's airflow.models.connection.Connection.get
	// behaviour. Pairs with the "TODO: mask secrets here" hook in
	// pkg/worker/runner.go's task log handler and the matching TODO on
	// GetVariable above.

	return conn, nil
}

// GetXCom requests an XCom value from the supervisor.
func (c *CoordinatorClient) GetXCom(
	ctx context.Context,
	dagId, runId, taskId string,
	mapIndex *int,
	key string,
	_ any,
) (any, error) {
	msg := genmodels.GetXCom{
		Key:    key,
		DagID:  dagId,
		TaskID: taskId,
		RunID:  runId,
	}
	// Assign the pointer, not the dereferenced int: map_index is a nullable
	// interface{} field and msgpack's omitempty treats an interface{} holding
	// int(0) as empty, dropping an explicit map_index 0 so the supervisor would
	// read mapped index 0 as unmapped. A *int in the interface encodes its pointee
	// (0 included), and a nil pointer is still omitted.
	msg.MapIndex = mapIndex

	resp, err := c.comm.Communicate(ctx, msg)
	if err != nil {
		return nil, translateApiError(err, errCodeXComNotFound, sdk.XComNotFound, key)
	}

	var result genmodels.XComResult
	if err := decodeBody(resp, &result); err != nil {
		return nil, fmt.Errorf("decoding xcom result: %w", err)
	}

	return result.Value, nil
}

// PushXCom sends an XCom value to the supervisor.
func (c *CoordinatorClient) PushXCom(
	ctx context.Context,
	ti api.TaskInstance,
	key string,
	value any,
) error {
	msg := genmodels.SetXCom{
		Key:    key,
		Value:  value,
		DagID:  ti.DagId,
		TaskID: ti.TaskId,
		RunID:  ti.RunId,
	}
	// map_index mirrors Python's SetXCom.map_index (int | None): -1 is the
	// unmapped sentinel, omitted from the payload rather than sent. Assign the
	// pointer, not the dereferenced int, so an explicit index 0 survives omitempty
	// (see GetXCom).
	if ti.MapIndex != nil && *ti.MapIndex != -1 {
		msg.MapIndex = ti.MapIndex
	}

	_, err := c.comm.Communicate(ctx, msg)
	return err
}
