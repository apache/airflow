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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"

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
	errCodeTaskStoreNotFound  = "TASK_STORE_NOT_FOUND"
)

// A language SDK runtime cannot read Airflow config, so the supervisor passes
// this setting at launch (task-sdk/src/airflow/sdk/coordinators/_subprocess.py).
const defaultRetentionDaysEnv = "AIRFLOW__STATE_STORE__DEFAULT_RETENTION_DAYS"

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
	// Bound at construction, not per call: the Execution API scopes the task
	// state store to the caller's own task instance ("ti:self").
	tiID string
}

var _ sdk.Client = (*CoordinatorClient)(nil)

// NewCoordinatorClient creates a new client backed by the comm socket.
func NewCoordinatorClient(comm *CoordinatorComm, tiID string) *CoordinatorClient {
	return &CoordinatorClient{
		comm: comm,
		tiID: tiID,
	}
}

// resolveDefaultExpiry returns nil ("never expires") for a retention of 0. The
// supervisor always passes the setting, so an absent or malformed value is a
// misconfiguration and fails the write rather than silently retaining the key
// for a period nobody configured.
func resolveDefaultExpiry(now time.Time) (any, error) {
	raw, ok := os.LookupEnv(defaultRetentionDaysEnv)
	if !ok {
		return nil, fmt.Errorf(
			"%s is not set; it carries the deployment's %q key in %q section to the runtime",
			defaultRetentionDaysEnv,
			"default_retention_days",
			"state_store",
		)
	}
	days, err := parseRetentionDays(raw)
	if err != nil {
		return nil, err
	}
	if days == 0 {
		return nil, nil
	}
	expiry := now.UTC().AddDate(0, 0, days)
	// A day count big enough to wrap the timestamp would store a key that is
	// already expired, losing it on the next cleanup; Python raises
	// OverflowError on the same setting.
	if !expiry.After(now.UTC()) {
		return nil, fmt.Errorf(
			"a retention of %d days overflows the expiry timestamp. Please check %q key in %q section",
			days,
			"default_retention_days",
			"state_store",
		)
	}
	return expiry, nil
}

// parseRetentionDays accepts "7.0" because Python's getint does.
func parseRetentionDays(raw string) (int, error) {
	days, err := strconv.Atoi(raw)
	if err != nil {
		f, floatErr := strconv.ParseFloat(raw, 64)
		// A float outside int64 range (an infinity included) converts with an
		// implementation-defined result, so it is rejected rather than turned
		// into whatever day count this platform happens to produce.
		if floatErr != nil || f != math.Trunc(f) || f >= math.MaxInt64 || f <= math.MinInt64 {
			return 0, fmt.Errorf(
				"failed to convert value to int. Please check %q key in %q section. Current value: %q",
				"default_retention_days",
				"state_store",
				raw,
			)
		}
		days = int(f)
	}
	if days < 0 {
		return 0, fmt.Errorf(
			"[state_store] default_retention_days must be >= 0, got %d. "+
				"Set to 0 to disable expiry.",
			days,
		)
	}
	return days, nil
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

// TaskStateStore returns the task state store scoped to this task instance.
func (c *CoordinatorClient) TaskStateStore() sdk.TaskStateStore {
	return taskStateStore{client: c}
}

// taskStateStore serves sdk.TaskStateStore over the coordinator comm.
type taskStateStore struct {
	client *CoordinatorClient
}

// Get asks the supervisor for a task state value.
func (s taskStateStore) Get(ctx context.Context, key string) (any, error) {
	resp, err := s.client.comm.Communicate(
		ctx,
		genmodels.GetTaskStateStore{TIID: s.client.tiID, Key: key},
	)
	if err != nil {
		return nil, translateAPIError(err, errCodeTaskStoreNotFound, sdk.TaskStateNotFound, key)
	}

	var result genmodels.TaskStateStoreResult
	if err := decodeBody(resp, &result); err != nil {
		return nil, fmt.Errorf("decoding task state result: %w", err)
	}

	return result.Value, nil
}

// UnmarshalJSONValue gets a task state value and unmarshals it into pointer.
func (s taskStateStore) UnmarshalJSONValue(
	ctx context.Context,
	key string,
	pointer any,
) error {
	val, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	// The value arrives already decoded from msgpack, not as JSON text, so it
	// is re-marshaled before encoding/json can fill a typed pointer.
	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshaling task state value: %w", err)
	}
	return json.Unmarshal(b, pointer)
}

// Set asks the supervisor to store a task state value.
func (s taskStateStore) Set(
	ctx context.Context,
	key string,
	value any,
	opts ...sdk.SetOption,
) error {
	var options sdk.SetOptions
	for _, opt := range opts {
		opt(&options)
	}

	// The caller's value is checked first so a misconfigured deployment cannot
	// mask a programming error in the task.
	if err := validateJSONRepresentable(value); err != nil {
		return fmt.Errorf("cannot set task state key %q: %w", key, err)
	}

	expiry, err := resolveExpiry(options.Retention, time.Now())
	if err != nil {
		return fmt.Errorf("cannot set task state key %q: %w", key, err)
	}

	// TODO: warn when the serialized value exceeds the deployment's
	// [state_store] max_value_storage_bytes, matching Python's
	// airflow.sdk.execution_time.context task store setter.

	_, err = s.client.comm.Communicate(ctx, genmodels.SetTaskStateStore{
		TIID:      s.client.tiID,
		Key:       key,
		Value:     value,
		ExpiresAt: expiry,
	})
	return err
}

// Delete asks the supervisor to delete a task state value.
func (s taskStateStore) Delete(ctx context.Context, key string) error {
	_, err := s.client.comm.Communicate(
		ctx,
		genmodels.DeleteTaskStateStore{TIID: s.client.tiID, Key: key},
	)
	return err
}

// Clear asks the supervisor to delete every task state value for this task
// instance.
func (s taskStateStore) Clear(ctx context.Context) error {
	_, err := s.client.comm.Communicate(
		ctx,
		genmodels.ClearTaskStateStore{TIID: s.client.tiID},
	)
	return err
}

// resolveExpiry turns a retention into the wire expires_at. A nil retention -
// no sdk.WithRetention - follows the deployment default.
func resolveExpiry(retention *time.Duration, now time.Time) (any, error) {
	if retention == nil {
		return resolveDefaultExpiry(now)
	}
	switch r := *retention; {
	// Checked before any arithmetic: adding NeverExpire overflows.
	case r == sdk.NeverExpire:
		return nil, nil
	case r <= 0:
		return nil, fmt.Errorf(
			"retention must be positive or sdk.NeverExpire, got %s: omit "+
				"sdk.WithRetention to follow the deployment default, or call Delete to drop the key",
			r,
		)
	default:
		return now.UTC().Add(r), nil
	}
}

// validateJSONRepresentable checks what the frame encoder actually emits, not
// the Go value: a reflection walk has to mirror the encoder's field rules (tags,
// "-", omitempty, embedding, marshalers) and misjudges values wherever it drifts.
func validateJSONRepresentable(value any) error {
	var buf bytes.Buffer
	if err := newFrameEncoder(&buf).Encode(value); err != nil {
		return fmt.Errorf("%T is not JSON representable: %w", value, err)
	}
	dec := msgpack.NewDecoder(&buf)
	// The default map decoder fails opaquely on non-string keys.
	dec.SetMapDecoder(func(d *msgpack.Decoder) (any, error) {
		return d.DecodeUntypedMap()
	})
	decoded, err := dec.DecodeInterface()
	if err != nil {
		return fmt.Errorf("%T is not JSON representable: %w", value, err)
	}
	// Checked after decoding: a typed nil such as a nil *string is not == nil,
	// yet it still encodes to null, which the Execution API rejects.
	if decoded == nil {
		return errors.New("value must not be nil")
	}
	return validateDecodedValue(decoded)
}

func validateDecodedValue(v any) error {
	switch v := v.(type) {
	case nil, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return nil
	case float32:
		return checkFinite(float64(v))
	case float64:
		return checkFinite(v)
	case time.Time:
		return fmt.Errorf(
			"time.Time is not JSON representable; store value.Format(time.RFC3339) " +
				"and parse it back with time.Parse",
		)
	case []byte:
		return fmt.Errorf(
			"[]byte is not JSON representable; encode it, for example with base64.StdEncoding.EncodeToString",
		)
	case []any:
		for _, elem := range v {
			if err := validateDecodedValue(elem); err != nil {
				return err
			}
		}
		return nil
	case map[any]any:
		for k, elem := range v {
			if _, ok := k.(string); !ok {
				return fmt.Errorf("map keys must be strings, got %T", k)
			}
			if err := validateDecodedValue(elem); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%T is not JSON representable", v)
	}
}

func checkFinite(f float64) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("value must be a finite number; NaN and Inf are not JSON representable")
	}
	return nil
}
