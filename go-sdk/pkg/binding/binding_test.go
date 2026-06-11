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

package binding

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

type extractResult struct {
	GoVersion string `json:"go_version"`
	Timestamp int64  `json:"timestamp"`
}

type transformInput struct {
	Extracted extractResult `xcom:"extract"`
}

// xcomCall records one GetXCom invocation so tests can assert the runtime
// pulled the right upstream/key for the current dag run.
type xcomCall struct {
	dagID, runID, taskID, key string
}

// fakeClient is a minimal sdk.Client whose GetXCom returns preconfigured values
// keyed by "<taskID>/<key>"; absent entries yield sdk.XComNotFound.
type fakeClient struct {
	xcoms map[string]any
	calls []xcomCall
}

var _ sdk.Client = (*fakeClient)(nil)

func (c *fakeClient) GetXCom(
	_ context.Context,
	dagID, runID, taskID string,
	_ *int,
	key string,
	_ any,
) (any, error) {
	c.calls = append(c.calls, xcomCall{dagID, runID, taskID, key})
	if v, ok := c.xcoms[taskID+"/"+key]; ok {
		return v, nil
	}
	return nil, sdk.XComNotFound
}

func (c *fakeClient) PushXCom(context.Context, api.TaskInstance, string, any) error { return nil }

func (c *fakeClient) GetVariable(
	context.Context,
	string,
) (string, error) {
	return "", nil
}
func (c *fakeClient) UnmarshalJSONVariable(context.Context, string, any) error { return nil }
func (c *fakeClient) GetConnection(context.Context, string) (sdk.Connection, error) {
	return sdk.Connection{}, nil
}

type BindingSuite struct {
	suite.Suite
}

func TestBindingSuite(t *testing.T) {
	suite.Run(t, &BindingSuite{})
}

func (s *BindingSuite) TestParseXComTag() {
	cases := map[string]struct {
		tag         string
		taskID, key string
		errContains string
	}{
		"task-only":     {tag: "extract", taskID: "extract", key: "return_value"},
		"task-and-key":  {tag: "extract,key=foo", taskID: "extract", key: "foo"},
		"trims-spaces":  {tag: " extract , key=bar ", taskID: "extract", key: "bar"},
		"empty":         {tag: "", errContains: "missing the upstream task id"},
		"empty-task":    {tag: ",key=foo", errContains: "missing the upstream task id"},
		"unknown-opt":   {tag: "extract,bogus", errContains: `unknown xcom tag option "bogus"`},
		"empty-key-opt": {tag: "extract,key=", errContains: "key= is empty"},
	}
	for name, tt := range cases {
		s.Run(name, func() {
			taskID, key, err := parseXComTag(tt.tag)
			if tt.errContains != "" {
				if s.Error(err) {
					s.Contains(err.Error(), tt.errContains)
				}
				return
			}
			s.NoError(err)
			s.Equal(tt.taskID, taskID)
			s.Equal(tt.key, key)
		})
	}
}

func (s *BindingSuite) TestAnalyze() {
	cases := map[string]struct {
		fn          any
		errContains string
	}{
		"injectables": {
			fn: func(context.Context, *slog.Logger, sdk.Client) error { return nil },
		},
		"valid-input-struct": {
			fn: func(in transformInput) error { return nil },
		},
		"untagged-field": {
			fn:          func(in struct{ Extracted extractResult }) error { return nil },
			errContains: "has no `xcom` tag",
		},
		"empty-tag": {
			fn: func(in struct {
				X string `xcom:""`
			},
			) error {
				return nil
			},
			errContains: "missing the upstream task id",
		},
		"undecodable-field": {
			fn: func(in struct {
				X chan int `xcom:"up"`
			},
			) error {
				return nil
			},
			errContains: "cannot hold an xcom value",
		},
		"non-struct-param": {
			fn:          func(x int) error { return nil },
			errContains: "is not an injectable type",
		},
		"no-tagged-fields": {
			fn:          func(in struct{ unexported int }) error { return nil },
			errContains: "no exported `xcom`-tagged fields",
		},
	}
	for name, tt := range cases {
		s.Run(name, func() {
			_, err := Analyze(reflect.TypeOf(tt.fn), "fn")
			if tt.errContains != "" {
				if s.Error(err) {
					s.Contains(err.Error(), tt.errContains)
				}
				return
			}
			s.NoError(err)
		})
	}
}

func (s *BindingSuite) TestDecodeXCom() {
	s.Run("strict-struct-success", func() {
		v, err := decodeXCom(
			map[string]any{"go_version": "go1.24", "timestamp": 7},
			reflect.TypeFor[extractResult](),
		)
		s.Require().NoError(err)
		got := v.Interface().(extractResult)
		s.Equal("go1.24", got.GoVersion)
		s.Equal(int64(7), got.Timestamp)
	})
	s.Run("strict-struct-unknown-field-fails", func() {
		_, err := decodeXCom(
			map[string]any{"go_version": "x", "renamed": 1},
			reflect.TypeFor[extractResult](),
		)
		s.Error(err)
	})
	s.Run("loose-map-accepts-any-shape", func() {
		v, err := decodeXCom(
			map[string]any{"anything": 1, "else": "ok"},
			reflect.TypeFor[map[string]any](),
		)
		s.Require().NoError(err)
		s.Len(v.Interface().(map[string]any), 2)
	})
	s.Run("string-into-struct-fails", func() {
		_, err := decodeXCom("hello", reflect.TypeFor[extractResult]())
		s.Error(err)
	})
	s.Run("null-into-struct-fails", func() {
		_, err := decodeXCom(nil, reflect.TypeFor[extractResult]())
		if s.Error(err) {
			s.Contains(err.Error(), "not nilable")
		}
	})
	s.Run("null-into-map-ok", func() {
		v, err := decodeXCom(nil, reflect.TypeFor[map[string]any]())
		s.Require().NoError(err)
		s.Nil(v.Interface())
	})
}

// resolve runs Analyze + Resolve against a fake client, with a workload in
// context so xcom pulls can read the current dag/run.
func (s *BindingSuite) resolve(fn any, client *fakeClient) ([]reflect.Value, error) {
	plan, err := Analyze(reflect.TypeOf(fn), "fn")
	s.Require().NoError(err)

	workload := api.ExecuteTaskWorkload{
		TI: api.TaskInstance{DagId: "dag1", RunId: "run1", TaskId: "transform"},
	}
	ctx := context.WithValue(context.Background(), sdkcontext.WorkloadContextKey, workload)
	return plan.Resolve(ctx, slog.Default(), client)
}

func (s *BindingSuite) TestResolveInjectsXComInput() {
	client := &fakeClient{xcoms: map[string]any{
		"extract/return_value": map[string]any{"go_version": "go1.24", "timestamp": 99},
	}}

	args, err := s.resolve(func(context.Context, transformInput) error { return nil }, client)

	s.Require().NoError(err)
	s.Require().Len(args, 2)
	got := args[1].Interface().(transformInput)
	s.Equal("go1.24", got.Extracted.GoVersion)
	s.Equal(int64(99), got.Extracted.Timestamp)
	s.Require().Len(client.calls, 1)
	s.Equal(xcomCall{"dag1", "run1", "extract", "return_value"}, client.calls[0])
}

func (s *BindingSuite) TestResolveCustomKeyAndPointer() {
	type pyInput struct {
		FromPython string `xcom:"python_task_1,key=out"`
	}
	client := &fakeClient{xcoms: map[string]any{
		"python_task_1/out": "value_from_python",
	}}

	args, err := s.resolve(func(*pyInput) error { return nil }, client)

	s.Require().NoError(err)
	s.Require().Len(args, 1)
	got := args[0].Interface().(*pyInput)
	s.Require().NotNil(got)
	s.Equal("value_from_python", got.FromPython)
	s.Equal(xcomCall{"dag1", "run1", "python_task_1", "out"}, client.calls[0])
}

func (s *BindingSuite) TestResolveMissingUpstreamFails() {
	client := &fakeClient{xcoms: map[string]any{}} // extract not present

	_, err := s.resolve(func(transformInput) error { return nil }, client)

	s.Require().Error(err)
	s.True(errors.Is(err, sdk.XComNotFound))
}
