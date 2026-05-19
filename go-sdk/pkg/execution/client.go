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
	"fmt"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/sdk"
)

// CoordinatorClient implements sdk.Client by communicating with the Airflow supervisor
// over the comm socket using msgpack-framed IPC instead of HTTP.
type CoordinatorClient struct {
	comm    *CoordinatorComm
	details *StartupDetails
}

var _ sdk.Client = (*CoordinatorClient)(nil)

// NewCoordinatorClient creates a new client backed by the comm socket.
func NewCoordinatorClient(comm *CoordinatorComm, details *StartupDetails) *CoordinatorClient {
	return &CoordinatorClient{
		comm:    comm,
		details: details,
	}
}

// GetVariable requests a variable value from the supervisor.
func (c *CoordinatorClient) GetVariable(_ context.Context, key string) (string, error) {
	resp, err := c.comm.Communicate(GetVariableMsg{Key: key}.toMap())
	if err != nil {
		return "", err
	}

	result, err := decodeVariableResult(resp)
	if err != nil {
		return "", fmt.Errorf("decoding variable result: %w", err)
	}

	if result.Value == nil {
		return "", fmt.Errorf("%w: %q", sdk.VariableNotFound, key)
	}

	switch v := result.Value.(type) {
	case string:
		return v, nil
	default:
		// If the value is not a string, marshal it to JSON.
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
	_ context.Context,
	connID string,
) (sdk.Connection, error) {
	resp, err := c.comm.Communicate(GetConnectionMsg{ConnID: connID}.toMap())
	if err != nil {
		return sdk.Connection{}, err
	}

	result, err := decodeConnectionResult(resp)
	if err != nil {
		return sdk.Connection{}, fmt.Errorf("decoding connection result: %w", err)
	}

	conn := sdk.Connection{
		ID:   result.ConnID,
		Type: result.ConnType,
		Host: result.Host,
		Port: result.Port,
		Path: result.Schema,
	}

	if result.Login != "" {
		login := result.Login
		conn.Login = &login
	}
	if result.Password != "" {
		password := result.Password
		conn.Password = &password
	}
	if result.Extra != "" {
		conn.Extra = map[string]any{}
		if err := json.Unmarshal([]byte(result.Extra), &conn.Extra); err != nil {
			return conn, fmt.Errorf("parsing connection extra: %w", err)
		}
	}

	return conn, nil
}

// GetXCom requests an XCom value from the supervisor.
func (c *CoordinatorClient) GetXCom(
	_ context.Context,
	dagId, runId, taskId string,
	mapIndex *int,
	key string,
	_ any,
) (any, error) {
	msg := GetXComMsg{
		Key:    key,
		DagID:  dagId,
		TaskID: taskId,
		RunID:  runId,
	}
	if mapIndex != nil {
		msg.MapIndex = mapIndex
	}

	resp, err := c.comm.Communicate(msg.toMap())
	if err != nil {
		return nil, err
	}

	result, err := decodeXComResult(resp)
	if err != nil {
		return nil, fmt.Errorf("decoding xcom result: %w", err)
	}

	return result.Value, nil
}

// PushXCom sends an XCom value to the supervisor.
func (c *CoordinatorClient) PushXCom(
	_ context.Context,
	ti api.TaskInstance,
	key string,
	value any,
) error {
	mapIndex := -1
	if ti.MapIndex != nil && *ti.MapIndex != -1 {
		mapIndex = *ti.MapIndex
	}

	msg := SetXComMsg{
		Key:      key,
		Value:    value,
		DagID:    ti.DagId,
		TaskID:   ti.TaskId,
		RunID:    ti.RunId,
		MapIndex: mapIndex,
	}

	_, err := c.comm.Communicate(msg.toMap())
	return err
}
