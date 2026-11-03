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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// withLocalZone runs the test with a non-UTC time.Local, so a timestamp that
// msgpack decodes in the local zone is distinguishable from one in UTC.
func withLocalZone(t *testing.T) {
	t.Helper()
	orig := time.Local
	time.Local = time.FixedZone("UTC+8", 8*60*60)
	t.Cleanup(func() { time.Local = orig })
}

func TestTimeDecodeMsgpack_UTC(t *testing.T) {
	withLocalZone(t)
	want := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	raw, err := msgpack.Marshal(want)
	require.NoError(t, err)

	var got Time
	require.NoError(t, msgpack.Unmarshal(raw, &got))

	assert.Equal(t, want, time.Time(got))
	assert.Equal(t, time.UTC, time.Time(got).Location())
}

func TestTimeDecodeMsgpack_String(t *testing.T) {
	withLocalZone(t)
	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := map[string]struct {
		wire    string
		wantErr bool
	}{
		"RFC 3339 Z":       {wire: "2026-01-02T03:04:05Z"},
		"offset +00:00":    {wire: "2026-01-02T03:04:05+00:00"},
		"offset +08:00":    {wire: "2026-01-02T11:04:05+08:00"},
		"naive is refused": {wire: "2026-01-02T03:04:05", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			raw, err := msgpack.Marshal(tc.wire)
			require.NoError(t, err)

			var got Time
			err = msgpack.Unmarshal(raw, &got)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, want, time.Time(got))
			assert.Equal(t, time.UTC, time.Time(got).Location())
		})
	}
}

func TestTimeDecodeMsgpack_Nil(t *testing.T) {
	raw, err := msgpack.Marshal(map[string]any{"value": nil, "nullable": nil})
	require.NoError(t, err)

	var got struct {
		Value    Time  `msgpack:"value"`
		Nullable *Time `msgpack:"nullable"`
	}
	got.Nullable = new(Time)
	require.NoError(t, msgpack.Unmarshal(raw, &got))

	assert.True(t, time.Time(got.Value).IsZero())
	assert.Nil(t, got.Nullable)
}

func TestTimeEncodeMsgpack_OmitEmpty(t *testing.T) {
	type omitEmpty struct {
		Value Time `msgpack:"value,omitempty"`
	}
	want := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	tests := map[string]struct {
		value    Time
		wantKeys []string
	}{
		"set is encoded":  {value: Time(want), wantKeys: []string{"value"}},
		"zero is omitted": {value: Time{}, wantKeys: []string{}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			raw, err := msgpack.Marshal(omitEmpty{Value: tc.value})
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, msgpack.Unmarshal(raw, &got))

			keys := []string{}
			for k := range got {
				keys = append(keys, k)
			}
			assert.Equal(t, tc.wantKeys, keys)
		})
	}
}

func TestTimeString(t *testing.T) {
	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	assert.Equal(t, want.String(), Time(want).String())
	assert.Equal(t, want.String(), fmt.Sprint(Time(want)))
}

func TestTimeEncodeMsgpack_Timestamp(t *testing.T) {
	want := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	nullable := Time(want)
	raw, err := msgpack.Marshal(struct {
		Value    Time  `msgpack:"value"`
		Nullable *Time `msgpack:"nullable"`
		Null     *Time `msgpack:"null"`
	}{Value: Time(want), Nullable: &nullable})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, msgpack.Unmarshal(raw, &got))

	for _, key := range []string{"value", "nullable"} {
		ts, ok := got[key].(time.Time)
		require.True(t, ok, "%s must encode as a timestamp, got %T", key, got[key])
		assert.True(t, want.Equal(ts), key)
	}
	assert.Nil(t, got["null"])
}

func TestGeneratedDateTimeFieldsDecodeUTC(t *testing.T) {
	withLocalZone(t)
	runAfter := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	logical := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	raw, err := msgpack.Marshal(map[string]any{
		"run_after":    runAfter,
		"logical_date": logical,
		"end_date":     nil,
	})
	require.NoError(t, err)

	var got DagRun
	require.NoError(t, msgpack.Unmarshal(raw, &got))

	assert.Equal(t, runAfter, time.Time(got.RunAfter))
	require.NotNil(t, got.LogicalDate)
	assert.Equal(t, logical, time.Time(*got.LogicalDate))
	assert.Nil(t, got.EndDate)
}

func TestGeneratedDateTimeListDecodesUTC(t *testing.T) {
	withLocalZone(t)
	dates := []time.Time{
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	raw, err := msgpack.Marshal(map[string]any{"dag_id": "d", "logical_dates": dates})
	require.NoError(t, err)

	var got GetDRCount
	require.NoError(t, msgpack.Unmarshal(raw, &got))

	require.NotNil(t, got.LogicalDates)
	require.Len(t, *got.LogicalDates, len(dates))
	for i, want := range dates {
		assert.Equal(t, want, time.Time((*got.LogicalDates)[i]))
	}
}
