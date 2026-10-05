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

// translateAPIError converts a supervisor *APIError whose Err field matches
// code into a sentinel-wrapped error. Any other error - including a
// *APIError with a different code - is returned unchanged so callers can keep
// distinguishing transport / server errors from "thing not found".
func translateAPIError(err error, code string, sentinel error, key string) error {
	if err == nil {
		return nil
	}
	var apiErr *APIError
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
	if env, ok := os.LookupEnv(sdk.VariableEnvPrefix + strings.ToUpper(key)); ok {
		return env, nil
	}

	resp, err := c.comm.Communicate(
		ctx,
		genmodels.GetVariable{Key: key},
	)
	if err != nil {
		return "", translateAPIError(err, errCodeVariableNotFound, sdk.VariableNotFound, key)
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

// SetVariable asks the supervisor to store a variable value.
func (c *CoordinatorClient) SetVariable(
	ctx context.Context,
	key, value, description string,
) error {
	msg := genmodels.PutVariable{Key: key, Value: value}
	if description != "" {
		msg.Description = description
	}
	_, err := c.comm.Communicate(ctx, msg)
	return err
}

// DeleteVariable asks the supervisor to delete a variable.
func (c *CoordinatorClient) DeleteVariable(ctx context.Context, key string) error {
	_, err := c.comm.Communicate(ctx, genmodels.DeleteVariable{Key: key})
	return err
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
		return sdk.Connection{}, translateAPIError(
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
	// way it does in Airflow. The supervisor schema types these as
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
	// behaviour.

	return conn, nil
}

// GetXCom requests an XCom value from the supervisor.
func (c *CoordinatorClient) GetXCom(
	ctx context.Context,
	dagID, runID, taskID string,
	mapIndex *int,
	key string,
	_ any,
) (any, error) {
	msg := genmodels.GetXCom{
		Key:    key,
		DagID:  dagID,
		TaskID: taskID,
		RunID:  runID,
	}
	// Assign the pointer, not the dereferenced int: map_index is a nullable
	// interface{} field and msgpack's omitempty treats an interface{} holding
	// int(0) as empty, dropping an explicit map_index 0 so the supervisor would
	// read mapped index 0 as unmapped. A *int in the interface encodes its pointee
	// (0 included), and a nil pointer is still omitted.
	msg.MapIndex = mapIndex

	resp, err := c.comm.Communicate(ctx, msg)
	if err != nil {
		return nil, translateAPIError(err, errCodeXComNotFound, sdk.XComNotFound, key)
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
	ti sdk.TaskInstance,
	key string,
	value any,
) error {
	msg := genmodels.SetXCom{
		Key:    key,
		Value:  value,
		DagID:  ti.DagID,
		TaskID: ti.TaskID,
		RunID:  ti.RunID,
	}
	msg.MapIndex = omittedMapIndex(ti.MapIndex)

	_, err := c.comm.Communicate(ctx, msg)
	return err
}

// deleteXCom asks the supervisor to delete the XCom of ti with the given key. Like PushXCom, it
// leaves map_index out for an unmapped task instance, and the Execution API then deletes the XCom
// with map_index -1.
func (c *CoordinatorClient) deleteXCom(ctx context.Context, ti sdk.TaskInstance, key string) error {
	msg := genmodels.DeleteXCom{
		Key:    key,
		DagID:  ti.DagID,
		TaskID: ti.TaskID,
		RunID:  ti.RunID,
	}
	msg.MapIndex = omittedMapIndex(ti.MapIndex)

	_, err := c.comm.Communicate(ctx, msg)
	return err
}

// omittedMapIndex returns mapIndex, or nil for the unmapped sentinel -1, so that msgpack omits
// map_index from the payload instead of sending it. An explicit index 0 survives omitempty because
// the pointer, not the dereferenced int, is returned (see GetXCom).
func omittedMapIndex(mapIndex *int) *int {
	if mapIndex == nil || *mapIndex == -1 {
		return nil
	}
	return mapIndex
}

// skipDownstreamTasks asks the supervisor to mark the tasks with the given task_ids as skipped
// in the Dag run of the running task. Airflow does not change a task instance that is running,
// has succeeded or has failed.
func (c *CoordinatorClient) skipDownstreamTasks(ctx context.Context, taskIDs []string) error {
	_, err := c.comm.Communicate(ctx, genmodels.SkipDownstreamTasks{Tasks: taskIDs})
	return err
}
