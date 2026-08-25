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

package genmodels

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// ptr returns the address of v, for building the *T defaults expected below.
func ptr[T any](v T) *T { return &v }

// The generated DecodeMsgpack methods seed each struct's non-zero schema
// defaults before decoding; msgpack overwrites only keys present on the wire, so
// an omitted field keeps its default instead of decoding to its Go zero value.
// A field widened to a pointer (e.g. map_index) is the exception: it is not
// seeded and an omitted key decodes to nil, the natural "absent".

func TestTaskInstanceDecodeMsgpack_SeedsDefaults(t *testing.T) {
	tests := []struct {
		name         string
		wire         map[string]any
		wantMapIndex *int
		wantQueue    string
	}{
		{
			name: "omitted fields fall back to schema defaults",
			wire: map[string]any{
				"id":             "x",
				"task_id":        "t",
				"dag_id":         "d",
				"run_id":         "r",
				"dag_version_id": "v",
				"try_number":     1,
			},
			wantMapIndex: nil,
			wantQueue:    "default",
		},
		{
			name: "explicit values override the seeded defaults",
			wire: map[string]any{
				"id":             "x",
				"task_id":        "t",
				"dag_id":         "d",
				"run_id":         "r",
				"dag_version_id": "v",
				"try_number":     1,
				"map_index":      3,
				"queue":          "high",
			},
			wantMapIndex: ptr(3),
			wantQueue:    "high",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := msgpack.Marshal(tc.wire)
			require.NoError(t, err)
			var ti TaskInstance
			require.NoError(t, msgpack.Unmarshal(raw, &ti))
			assert.Equal(t, tc.wantMapIndex, ti.MapIndex)
			assert.Equal(t, tc.wantQueue, ti.Queue)
		})
	}
}

// A nested struct that owns a generated DecodeMsgpack still seeds its defaults:
// msgpack invokes the field's custom decoder, so StartupDetails.TI keeps the
// TaskInstance queue = "default" even when the wire omits it. The pointer-widened
// map_index is not seeded and stays nil.
func TestStartupDetailsDecodeMsgpack_SeedsNestedTaskInstanceDefault(t *testing.T) {
	wire := map[string]any{
		"bundle_info":        map[string]any{},
		"dag_rel_path":       "dag.py",
		"sentry_integration": "s",
		"start_date":         time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		"ti": map[string]any{
			"id":             "x",
			"task_id":        "t",
			"dag_id":         "d",
			"run_id":         "r",
			"dag_version_id": "v",
			"try_number":     1,
		},
		"ti_context": map[string]any{"dag_run": map[string]any{}, "max_tries": 0},
	}
	raw, err := msgpack.Marshal(wire)
	require.NoError(t, err)
	var sd StartupDetails
	require.NoError(t, msgpack.Unmarshal(raw, &sd))
	assert.Equal(t, "default", sd.TI.Queue)
	assert.Nil(t, sd.TI.MapIndex)
}

// PreviousTIResponse.map_index is a nullable scalar generated as interface{}
// with a schema default of -1. The generator seeds interface{} defaults too, so
// an omitted map_index nested in a PreviousTIResult keeps the -1 sentinel
// instead of decoding to nil; the pointer field routes through the pointee's
// generated decoder.
func TestPreviousTIResultDecodeMsgpack_SeedsNestedMapIndexDefault(t *testing.T) {
	tests := []struct {
		name string
		ti   map[string]any
		want any
	}{
		{
			name: "omitted map_index falls back to the -1 sentinel",
			ti:   map[string]any{"dag_id": "d", "run_id": "r", "task_id": "t", "try_number": 1},
			want: -1,
		},
		{
			name: "explicit map_index overrides the seeded default",
			ti: map[string]any{
				"dag_id":     "d",
				"run_id":     "r",
				"task_id":    "t",
				"try_number": 1,
				"map_index":  4,
			},
			want: 4,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := msgpack.Marshal(
				map[string]any{"type": "PreviousTIResult", "task_instance": tc.ti},
			)
			require.NoError(t, err)
			var result PreviousTIResult
			require.NoError(t, msgpack.Unmarshal(raw, &result))
			require.NotNil(t, result.TaskInstance)
			assert.EqualValues(t, tc.want, result.TaskInstance.MapIndex)
		})
	}
}

// Nullable scalars generated as interface{} have a nil Go zero, so the generator
// seeds even string and bool defaults to keep them distinct from "absent".
func TestInterfaceScalarDefaultsAreSeeded(t *testing.T) {
	t.Run("interface{} string state seeds when omitted", func(t *testing.T) {
		raw, err := msgpack.Marshal(map[string]any{})
		require.NoError(t, err)
		var msg DeferTask
		require.NoError(t, msgpack.Unmarshal(raw, &msg))
		assert.Equal(t, "deferred", msg.State)
	})
	t.Run("interface{} bool seeds true when omitted", func(t *testing.T) {
		raw, err := msgpack.Marshal(map[string]any{})
		require.NoError(t, err)
		var msg DagCallbackRequest
		require.NoError(t, msgpack.Unmarshal(raw, &msg))
		assert.Equal(t, true, msg.IsFailureCallback)
	})
}

// A nullable defaulted field decodes to nil for both an explicit null and an
// absent key, so the default must seed only on absence and leave an explicit
// null as nil.
func TestNullableDefault_ExplicitNullPreservedNotSeeded(t *testing.T) {
	t.Run("DagRunResult clear_number: explicit null stays nil", func(t *testing.T) {
		raw, err := msgpack.Marshal(map[string]any{"clear_number": nil})
		require.NoError(t, err)
		var msg DagRunResult
		require.NoError(t, msgpack.Unmarshal(raw, &msg))
		assert.Nil(t, msg.ClearNumber)
	})
	t.Run("PreviousTIResponse map_index: explicit null stays nil", func(t *testing.T) {
		raw, err := msgpack.Marshal(map[string]any{"map_index": nil})
		require.NoError(t, err)
		var msg PreviousTIResponse
		require.NoError(t, msgpack.Unmarshal(raw, &msg))
		assert.Nil(t, msg.MapIndex)
	})
	t.Run("CreateHITLDetailPayload multiple: explicit null stays nil", func(t *testing.T) {
		raw, err := msgpack.Marshal(map[string]any{"multiple": nil})
		require.NoError(t, err)
		var msg CreateHITLDetailPayload
		require.NoError(t, msgpack.Unmarshal(raw, &msg))
		assert.Nil(t, msg.Multiple)
	})
	t.Run("DagRunResult clear_number: absent seeds the default", func(t *testing.T) {
		raw, err := msgpack.Marshal(map[string]any{})
		require.NoError(t, err)
		var msg DagRunResult
		require.NoError(t, msgpack.Unmarshal(raw, &msg))
		assert.EqualValues(t, 0, msg.ClearNumber)
	})
}

// A named enum field (string-based) is seeded through a type conversion.
func TestEmailRequestDecodeMsgpack_SeedsEnumDefault(t *testing.T) {
	tests := []struct {
		name string
		wire map[string]any
		want EmailRequestEmailType
	}{
		{
			name: "omitted email_type defaults to failure",
			wire: map[string]any{},
			want: EmailRequestEmailTypeFailure,
		},
		{
			name: "explicit value overrides the seeded default",
			wire: map[string]any{"email_type": "retry"},
			want: EmailRequestEmailTypeRetry,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := msgpack.Marshal(tc.wire)
			require.NoError(t, err)
			var msg EmailRequest
			require.NoError(t, msgpack.Unmarshal(raw, &msg))
			assert.Equal(t, tc.want, msg.EmailType)
		})
	}
}

func TestGetAssetEventByAssetDecode_AscendingPointer(t *testing.T) {
	tests := []struct {
		name string
		wire map[string]any
		want *bool
	}{
		{
			name: "omitted ascending decodes to nil",
			wire: map[string]any{"name": "a", "uri": "u"},
			want: nil,
		},
		{
			name: "explicit false is preserved",
			wire: map[string]any{"name": "a", "uri": "u", "ascending": false},
			want: ptr(false),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := msgpack.Marshal(tc.wire)
			require.NoError(t, err)
			var msg GetAssetEventByAsset
			require.NoError(t, msgpack.Unmarshal(raw, &msg))
			assert.Equal(t, tc.want, msg.Ascending)
		})
	}
}

// Ascending has schema default true, so a plain bool + ,omitempty would drop an
// explicit false on encode and the supervisor would sort ascending anyway.
func TestGetAssetEventByAssetEncode_AscendingPointerPreservesExplicitFalse(t *testing.T) {
	tests := []struct {
		name      string
		ascending *bool
		wantKey   bool
		wantValue bool
	}{
		{
			name:      "nil omits ascending so the supervisor applies its default",
			ascending: nil,
			wantKey:   false,
		},
		{
			name:      "explicit false is encoded",
			ascending: ptr(false),
			wantKey:   true,
			wantValue: false,
		},
		{
			name:      "explicit true is encoded",
			ascending: ptr(true),
			wantKey:   true,
			wantValue: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := msgpack.Marshal(
				GetAssetEventByAsset{Name: "a", URI: "u", Ascending: tc.ascending},
			)
			require.NoError(t, err)
			var wire map[string]any
			require.NoError(t, msgpack.Unmarshal(raw, &wire))
			v, ok := wire["ascending"]
			assert.Equal(t, tc.wantKey, ok)
			if tc.wantKey {
				assert.Equal(t, tc.wantValue, v)
			}
		})
	}
}

// A concrete int + ,omitempty drops an explicit 0 on encode, so the supervisor
// would reapply its non-zero default (map_index -1, mistaking mapped index 0 for
// unmapped). Widening map_index to *int keeps ,omitempty meaning "absent" for nil
// while still encoding an explicit 0; this is the outbound counterpart to the
// inbound default seeding.
func TestGetPreviousTIEncode_MapIndexPointerPreservesExplicitZero(t *testing.T) {
	tests := []struct {
		name      string
		mapIndex  *int
		wantKey   bool
		wantValue int
	}{
		{
			name:     "nil omits map_index so the supervisor applies its default",
			mapIndex: nil,
			wantKey:  false,
		},
		{
			name:      "explicit 0 is encoded as mapped index 0",
			mapIndex:  ptr(0),
			wantKey:   true,
			wantValue: 0,
		},
		{name: "explicit index is encoded verbatim", mapIndex: ptr(5), wantKey: true, wantValue: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := msgpack.Marshal(
				GetPreviousTI{DagID: "d", TaskID: "t", MapIndex: tc.mapIndex},
			)
			require.NoError(t, err)
			var wire map[string]any
			require.NoError(t, msgpack.Unmarshal(raw, &wire))
			v, ok := wire["map_index"]
			assert.Equal(t, tc.wantKey, ok)
			if tc.wantKey {
				assert.EqualValues(t, tc.wantValue, v)
			}
		})
	}
}
