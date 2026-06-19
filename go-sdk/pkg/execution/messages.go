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
	"fmt"
	"time"
)

// SupervisorSchemaVersion is the dated AIP-72 supervisor wire-schema version
// this SDK's coordinator protocol is compiled against, in YYYY-MM-DD form. It
// is reported in a bundle's airflow-metadata manifest as
// sdk.supervisor_schema_version so the supervisor can downgrade outbound
// messages / upgrade inbound messages to a shape the bundle understands.
const SupervisorSchemaVersion = "2026-06-16"

// Inbound messages (Supervisor -> Runtime).

// TaskInstanceInfo holds task instance details from StartupDetails.
type TaskInstanceInfo struct {
	ID             string
	TaskID         string
	DagID          string
	RunID          string
	TryNumber      int
	DagVersionID   string
	MapIndex       int
	ContextCarrier map[string]any
}

func decodeTaskInstanceInfo(m map[string]any) (TaskInstanceInfo, error) {
	if m == nil {
		return TaskInstanceInfo{}, fmt.Errorf("nil task instance map")
	}
	id, err := mapString(m, "id")
	if err != nil {
		return TaskInstanceInfo{}, fmt.Errorf("ti.id: %w", err)
	}
	taskID, err := mapString(m, "task_id")
	if err != nil {
		return TaskInstanceInfo{}, fmt.Errorf("ti.task_id: %w", err)
	}
	dagID, err := mapString(m, "dag_id")
	if err != nil {
		return TaskInstanceInfo{}, fmt.Errorf("ti.dag_id: %w", err)
	}
	runID, err := mapString(m, "run_id")
	if err != nil {
		return TaskInstanceInfo{}, fmt.Errorf("ti.run_id: %w", err)
	}
	tryNumber, err := mapInt(m, "try_number")
	if err != nil {
		return TaskInstanceInfo{}, fmt.Errorf("ti.try_number: %w", err)
	}
	dagVersionID := mapStringOr(m, "dag_version_id", "")
	mapIndex := mapIntOr(m, "map_index", -1)
	contextCarrier := mapMap(m, "context_carrier")

	return TaskInstanceInfo{
		ID:             id,
		TaskID:         taskID,
		DagID:          dagID,
		RunID:          runID,
		TryNumber:      tryNumber,
		DagVersionID:   dagVersionID,
		MapIndex:       mapIndex,
		ContextCarrier: contextCarrier,
	}, nil
}

// BundleInfoMsg holds bundle identification from StartupDetails.
type BundleInfoMsg struct {
	Name    string
	Version string
}

func decodeBundleInfo(m map[string]any) BundleInfoMsg {
	if m == nil {
		return BundleInfoMsg{}
	}
	return BundleInfoMsg{
		Name:    mapStringOr(m, "name", ""),
		Version: mapStringOr(m, "version", ""),
	}
}

// TIRunContext holds the runtime context for a task instance.
type TIRunContext struct {
	LogicalDate       *time.Time
	DataIntervalStart *time.Time
	DataIntervalEnd   *time.Time
	MaxTries          int
	ShouldRetry       bool
}

func decodeTIRunContext(m map[string]any) (TIRunContext, error) {
	if m == nil {
		return TIRunContext{}, nil
	}
	// max_tries / should_retry live at the top level of ti_context and drive
	// the retry decision, so they must be read regardless of whether the
	// nested dag_run object is present.
	ctx := TIRunContext{
		MaxTries:    mapIntOr(m, "max_tries", 0),
		ShouldRetry: mapBoolOr(m, "should_retry", false),
	}
	// The scheduling timestamps live on the nested dag_run object in the
	// supervisor's TIRunContext schema (ti_context.dag_run.logical_date, ...),
	// not at the top level of ti_context. See task-sdk's
	// airflow.sdk.api.datamodels._generated.{TIRunContext,DagRun}.
	dagRun := mapMap(m, "dag_run")
	if dagRun == nil {
		return ctx, nil
	}
	for _, f := range []struct {
		key string
		dst **time.Time
	}{
		{"logical_date", &ctx.LogicalDate},
		{"data_interval_start", &ctx.DataIntervalStart},
		{"data_interval_end", &ctx.DataIntervalEnd},
	} {
		raw, present := dagRun[f.key]
		if !present || raw == nil {
			continue
		}
		t, err := asTime(raw)
		if err != nil {
			return TIRunContext{}, fmt.Errorf("ti_context.dag_run.%s: %w", f.key, err)
		}
		*f.dst = &t
	}
	return ctx, nil
}

// StartupDetails is sent by the supervisor to initiate task execution.
type StartupDetails struct {
	TI                TaskInstanceInfo
	DagRelPath        string
	BundleInfo        BundleInfoMsg
	StartDate         time.Time
	TIContext         TIRunContext
	SentryIntegration string
}

func decodeStartupDetails(m map[string]any) (*StartupDetails, error) {
	tiMap := mapMap(m, "ti")
	ti, err := decodeTaskInstanceInfo(tiMap)
	if err != nil {
		return nil, fmt.Errorf("decoding ti: %w", err)
	}

	dagRelPath := mapStringOr(m, "dag_rel_path", "")
	bundleInfo := decodeBundleInfo(mapMap(m, "bundle_info"))

	var startDate time.Time
	if raw, present := m["start_date"]; present && raw != nil {
		startDate, err = asTime(raw)
		if err != nil {
			return nil, fmt.Errorf("start_date: %w", err)
		}
	}

	tiContext, err := decodeTIRunContext(mapMap(m, "ti_context"))
	if err != nil {
		return nil, fmt.Errorf("decoding ti_context: %w", err)
	}
	sentryIntegration := mapStringOr(m, "sentry_integration", "")

	return &StartupDetails{
		TI:                ti,
		DagRelPath:        dagRelPath,
		BundleInfo:        bundleInfo,
		StartDate:         startDate,
		TIContext:         tiContext,
		SentryIntegration: sentryIntegration,
	}, nil
}

// Response types (for runtime-initiated requests).

// ConnectionResult is the response to GetConnection. Login and Password are
// nullable in the supervisor schema (None vs "" are distinct), so they are
// decoded as *string to preserve that distinction.
type ConnectionResult struct {
	ConnID   string
	ConnType string
	Host     string
	Schema   string
	Login    *string
	Password *string
	Port     int
	Extra    string
}

func decodeConnectionResult(m map[string]any) (*ConnectionResult, error) {
	return &ConnectionResult{
		ConnID:   mapStringOr(m, "conn_id", ""),
		ConnType: mapStringOr(m, "conn_type", ""),
		Host:     mapStringOr(m, "host", ""),
		Schema:   mapStringOr(m, "schema", ""),
		Login:    mapStringPtr(m, "login"),
		Password: mapStringPtr(m, "password"),
		Port:     mapIntOr(m, "port", 0),
		Extra:    mapStringOr(m, "extra", ""),
	}, nil
}

// VariableResult is the response to GetVariable.
type VariableResult struct {
	Key   string
	Value any
}

func decodeVariableResult(m map[string]any) (*VariableResult, error) {
	return &VariableResult{
		Key:   mapStringOr(m, "key", ""),
		Value: m["value"],
	}, nil
}

// XComResult is the response to GetXCom.
type XComResult struct {
	Key   string
	Value any
}

func decodeXComResult(m map[string]any) (*XComResult, error) {
	return &XComResult{
		Key:   mapStringOr(m, "key", ""),
		Value: m["value"],
	}, nil
}

// ErrorResponse represents an error returned by the supervisor.
type ErrorResponse struct {
	Error  string
	Detail any
}

func decodeErrorResponse(m map[string]any) *ErrorResponse {
	if m == nil {
		return nil
	}
	return &ErrorResponse{
		Error:  mapStringOr(m, "error", ""),
		Detail: m["detail"],
	}
}

// Outbound messages (Runtime -> Supervisor).

// GetConnectionMsg is sent to request a connection from the supervisor.
type GetConnectionMsg struct {
	ConnID string
}

func (m GetConnectionMsg) toMap() map[string]any {
	return map[string]any{
		"type":    "GetConnection",
		"conn_id": m.ConnID,
	}
}

// GetVariableMsg is sent to request a variable from the supervisor.
type GetVariableMsg struct {
	Key string
}

func (m GetVariableMsg) toMap() map[string]any {
	return map[string]any{
		"type": "GetVariable",
		"key":  m.Key,
	}
}

// GetXComMsg is sent to request an XCom value from the supervisor.
type GetXComMsg struct {
	Key               string
	DagID             string
	TaskID            string
	RunID             string
	MapIndex          *int
	IncludePriorDates bool
}

func (m GetXComMsg) toMap() map[string]any {
	result := map[string]any{
		"type":                "GetXCom",
		"key":                 m.Key,
		"dag_id":              m.DagID,
		"task_id":             m.TaskID,
		"run_id":              m.RunID,
		"include_prior_dates": m.IncludePriorDates,
	}
	if m.MapIndex != nil {
		result["map_index"] = *m.MapIndex
	}
	return result
}

// SetXComMsg is sent to set an XCom value. MapIndex mirrors Python's
// SetXCom.map_index (int | None): nil means "unmapped task", and is omitted
// from the wire payload rather than encoded as a -1 sentinel.
type SetXComMsg struct {
	Key          string
	Value        any
	DagID        string
	TaskID       string
	RunID        string
	MapIndex     *int
	MappedLength *int
}

func (m SetXComMsg) toMap() map[string]any {
	result := map[string]any{
		"type":    "SetXCom",
		"key":     m.Key,
		"value":   m.Value,
		"dag_id":  m.DagID,
		"task_id": m.TaskID,
		"run_id":  m.RunID,
	}
	if m.MapIndex != nil {
		result["map_index"] = *m.MapIndex
	}
	if m.MappedLength != nil {
		result["mapped_length"] = *m.MappedLength
	}
	return result
}

// SucceedTaskMsg is sent as a terminal message when a task succeeds.
type SucceedTaskMsg struct {
	EndDate      time.Time
	TaskOutlets  []any
	OutletEvents []any
}

func (m SucceedTaskMsg) toMap() map[string]any {
	taskOutlets := m.TaskOutlets
	if taskOutlets == nil {
		taskOutlets = []any{}
	}
	outletEvents := m.OutletEvents
	if outletEvents == nil {
		outletEvents = []any{}
	}
	return map[string]any{
		"type":          "SucceedTask",
		"end_date":      m.EndDate.UTC().Format(time.RFC3339Nano),
		"task_outlets":  taskOutlets,
		"outlet_events": outletEvents,
	}
}

// TaskState is the terminal non-success state reported via TaskStateMsg.
// The wire values match Python's TaskInstanceState enum (and the generated
// api.TerminalStateNonSuccess); we define a local typed string so call
// sites get compile-time checking and don't have to import pkg/api just
// for the constants.
type TaskState string

const (
	TaskStateFailed  TaskState = "failed"
	TaskStateRemoved TaskState = "removed"
	TaskStateSkipped TaskState = "skipped"
)

// TaskStateMsg is sent as a terminal message for failed/removed/skipped tasks.
type TaskStateMsg struct {
	State   TaskState
	EndDate time.Time
}

func (m TaskStateMsg) toMap() map[string]any {
	return map[string]any{
		"type":     "TaskState",
		"state":    string(m.State),
		"end_date": m.EndDate.UTC().Format(time.RFC3339Nano),
	}
}

// RetryTaskMsg is sent as a terminal message when a task fails but has retries.
type RetryTaskMsg struct {
	EndDate time.Time
	Reason  string
}

func (m RetryTaskMsg) toMap() map[string]any {
	return map[string]any{
		"type":         "RetryTask",
		"end_date":     m.EndDate.UTC().Format(time.RFC3339Nano),
		"retry_reason": m.Reason,
	}
}

// Message dispatch.

// decodeIncomingBody dispatches decoding of a body map based on its "type" field.
func decodeIncomingBody(m map[string]any) (any, error) {
	if m == nil {
		return nil, nil
	}
	typ, _ := m["type"].(string)
	switch typ {
	case "StartupDetails":
		return decodeStartupDetails(m)
	case "ConnectionResult":
		return decodeConnectionResult(m)
	case "VariableResult":
		return decodeVariableResult(m)
	case "XComResult":
		return decodeXComResult(m)
	case "ErrorResponse":
		return decodeErrorResponse(m), nil
	default:
		return nil, fmt.Errorf("unknown message type %q", typ)
	}
}

// asTime parses a time value that may be a time.Time (from msgpack timestamp ext)
// or a string (ISO 8601 format).
func asTime(v any) (time.Time, error) {
	if v == nil {
		return time.Time{}, fmt.Errorf("nil time value")
	}
	switch t := v.(type) {
	case time.Time:
		return t, nil
	case string:
		return time.Parse(time.RFC3339Nano, t)
	default:
		return time.Time{}, fmt.Errorf("expected time, got %T", v)
	}
}
