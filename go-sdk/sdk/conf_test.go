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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type etlTable struct {
	TableName string
}

type etlSource struct {
	BucketName string
}

type etlConf struct {
	TargetTable  string
	Limit        int
	Since        time.Time
	Source       *etlSource
	Tables       []etlTable
	TableArray   [1]etlTable
	TablesByName map[string]etlTable
	Options      map[string]any
}

type taggedConf struct {
	Table  string `json:"target_table"`
	Rows   int    `json:"row_count,omitempty"`
	Secret string `json:"-"`
}

type commonConf struct {
	Env   string
	Limit int
}

type jobConf struct {
	commonConf

	Limit int
	Name  string
}

type foldClashConf struct {
	TargetTable string
	Targettable string
}

func dagRunWithConf(conf map[string]any) DagRun {
	return DagRun{DagID: "dag1", RunID: "run1", Conf: conf}
}

func TestUnmarshalConfMatchesKeysIgnoringCaseAndUnderscores(t *testing.T) {
	var conf etlConf

	require.NoError(t, dagRunWithConf(map[string]any{
		"target_table": "sales",
		"LIMIT":        int8(5),
	}).UnmarshalConf(&conf))

	assert.Equal(t, "sales", conf.TargetTable)
	assert.Equal(t, 5, conf.Limit)
}

// Another task of the run may read a key this task does not declare, so an
// unmatched key is not an error, and the field keeps the default the task set.
func TestUnmarshalConfIgnoresUnmatchedKeysAndKeepsDefaults(t *testing.T) {
	conf := etlConf{Limit: 100}

	require.NoError(t, dagRunWithConf(map[string]any{
		"notify_email": "ops@example.com",
	}).UnmarshalConf(&conf))

	assert.Empty(t, conf.TargetTable)
	assert.Equal(t, 100, conf.Limit)
}

func TestUnmarshalConfPrefersAnExactKeyOverFoldedOnes(t *testing.T) {
	var conf etlConf

	require.NoError(t, dagRunWithConf(map[string]any{
		"target_table": "folded",
		"TargetTable":  "exact",
		"TARGET_TABLE": "folded",
	}).UnmarshalConf(&conf))

	assert.Equal(t, "exact", conf.TargetTable)
}

func TestUnmarshalConfMatchesKeysOfNestedObjects(t *testing.T) {
	var conf etlConf

	require.NoError(t, dagRunWithConf(map[string]any{
		"source":         map[string]any{"bucket_name": "raw"},
		"tables":         []any{map[string]any{"table_name": "orders"}},
		"table_array":    []any{map[string]any{"table_name": "items"}},
		"tables_by_name": map[string]any{"Sales_Key": map[string]any{"table_name": "sales"}},
	}).UnmarshalConf(&conf))

	require.NotNil(t, conf.Source)
	assert.Equal(t, "raw", conf.Source.BucketName)
	assert.Equal(t, []etlTable{{TableName: "orders"}}, conf.Tables)
	assert.Equal(t, [1]etlTable{{TableName: "items"}}, conf.TableArray)
	assert.Equal(t, map[string]etlTable{"Sales_Key": {TableName: "sales"}}, conf.TablesByName)
}

// The keys of a map are data, so they reach the task as the run was triggered
// with them, at every depth.
func TestUnmarshalConfKeepsTheKeysOfAMapWithUntypedValues(t *testing.T) {
	options := map[string]any{
		"Rows":      []any{map[string]any{"Key_A": "a"}},
		"Inner_Map": map[string]any{"Key_B": "b"},
	}
	var conf etlConf

	require.NoError(t, dagRunWithConf(map[string]any{"options": options}).UnmarshalConf(&conf))

	assert.Equal(t, options, conf.Options)
}

func TestUnmarshalConfReadsATimestampIntoATimeField(t *testing.T) {
	var conf etlConf

	require.NoError(t, dagRunWithConf(map[string]any{
		"since": "2024-01-31T02:00:00+08:00",
	}).UnmarshalConf(&conf))

	assert.True(
		t,
		conf.Since.Equal(time.Date(2024, 1, 31, 2, 0, 0, 0, time.FixedZone("", 8*60*60))),
	)
}

func TestUnmarshalConfMatchesAJSONTagAndSkipsASkippedField(t *testing.T) {
	var conf taggedConf

	require.NoError(t, dagRunWithConf(map[string]any{
		"TargetTable": "sales",
		"ROW_COUNT":   int8(5),
		"secret":      "token",
	}).UnmarshalConf(&conf))

	assert.Equal(t, "sales", conf.Table)
	assert.Equal(t, 5, conf.Rows)
	assert.Empty(t, conf.Secret, `a json:"-" field takes no conf key`)
}

func TestUnmarshalConfFillsPromotedFieldsAndHonoursShadowing(t *testing.T) {
	var conf jobConf

	require.NoError(t, dagRunWithConf(map[string]any{
		"env":   "prod",
		"limit": int8(5),
	}).UnmarshalConf(&conf))

	assert.Equal(t, "prod", conf.Env)
	assert.Equal(t, 5, conf.Limit)
	assert.Zero(t, conf.commonConf.Limit, "the outer field shadows the one it promotes")
}

func TestUnmarshalConfRejectsTwoKeysThatMatchOneField(t *testing.T) {
	var conf etlConf

	err := dagRunWithConf(map[string]any{
		"target_table": "a",
		"TARGET_TABLE": "b",
	}).UnmarshalConf(&conf)

	require.EqualError(
		t,
		err,
		`sdk.DagRun.UnmarshalConf: Dag run conf keys "TARGET_TABLE" and "target_table" both match field `+
			`etlConf.TargetTable when case and underscores are ignored, so the SDK cannot tell which one to `+
			`use; name the field or its json tag exactly as the key it should take`,
	)
}

// The struct, not the conf, is what has to be fixed, so the error does not
// wait for a run whose conf happens to hold one of the two names.
func TestUnmarshalConfRejectsFieldsThatDifferOnlyInCaseOrUnderscores(t *testing.T) {
	var conf foldClashConf

	err := DagRun{DagID: "dag1", RunID: "run1"}.UnmarshalConf(&conf)

	require.EqualError(
		t,
		err,
		"sdk.DagRun.UnmarshalConf: fields foldClashConf.TargetTable and foldClashConf.Targettable differ "+
			"only in case or underscores, so the SDK cannot tell which one a Dag run conf key should go "+
			"to; rename one of them",
	)
}

func TestUnmarshalConfNamesTheTargetWhenAValueDoesNotFit(t *testing.T) {
	var conf etlConf

	err := dagRunWithConf(map[string]any{"limit": "many"}).UnmarshalConf(&conf)

	require.ErrorContains(
		t,
		err,
		"sdk.DagRun.UnmarshalConf: cannot read the Dag run conf into sdk.etlConf: ",
	)
	require.ErrorContains(
		t,
		err,
		"cannot unmarshal string into Go struct field etlConf.Limit of type int",
	)
}

func TestUnmarshalConfRejectsATargetItCannotFill(t *testing.T) {
	var nilTarget *etlConf

	for _, target := range []any{etlConf{}, nilTarget, nil} {
		err := dagRunWithConf(map[string]any{"limit": int8(5)}).UnmarshalConf(target)

		require.ErrorContains(
			t,
			err,
			"sdk.DagRun.UnmarshalConf: target must be a non-nil pointer, got ",
		)
	}
}

// A run triggered without a conf leaves every field as the task set it.
func TestUnmarshalConfOfARunWithoutAConf(t *testing.T) {
	conf := etlConf{Limit: 100}

	require.NoError(t, DagRun{DagID: "dag1", RunID: "run1"}.UnmarshalConf(&conf))

	assert.Equal(t, etlConf{Limit: 100}, conf)
}
