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

package bundlev1

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/logging"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

type TaskSuite struct {
	suite.Suite
}

func TestTaskSuite(t *testing.T) {
	suite.Run(t, &TaskSuite{})
}

func (s *TaskSuite) TestReturnValidation() {
	cases := map[string]struct {
		fn          any
		errContains string
	}{
		"no-ret-values": {
			func() {},
			`func\d+ has 0 return values, must be`,
		},
		"too-many-ret-values": {
			func() (a, b, c int) { return },
			`func\d+ has 3 return values, must be`,
		},
		"invalid-ret": {
			func() (c chan int) { return },
			`func\d+ last return value to return error but found chan`,
		},
	}

	for name, tt := range cases {
		s.Run(name, func() {
			_, err := NewTaskFunction(tt.fn)
			if s.Assert().Error(err) {
				s.Assert().Regexp(tt.errContains, err.Error())
			}
		})
	}
}

func (s *TaskSuite) TestArgumentBinding() {
	cases := map[string]struct {
		fn any
	}{
		"no-args": {
			func() error { return nil },
		},
		"context": {
			func(ctx context.Context) error {
				s.Equal("def", ctx.Value("abc"))
				return nil
			},
		},
		"context-and-logger": {
			func(ctx context.Context, logger *slog.Logger) error {
				s.Equal("def", ctx.Value("abc"))
				s.NotNil(logger)
				return nil
			},
		},
		"client": {
			func(client sdk.Client) error {
				s.NotNil(client)
				return nil
			},
		},
		"var-client": {
			func(client sdk.VariableClient) error {
				s.NotNil(client)
				return nil
			},
		},
		"conn-client": {
			func(client sdk.ConnectionClient) error {
				s.NotNil(client)

				return nil
			},
		},
		"xcom-client": {
			func(client sdk.XComClient) error {
				s.NotNil(client)

				return nil
			},
		},
	}

	for name, tt := range cases {
		s.Run(name, func() {
			task, err := NewTaskFunction(tt.fn)
			s.Require().NoError(err)

			ctx := context.WithValue(context.Background(), "abc", "def")
			logger := slog.New(logging.NewTeeLogger())
			task.Execute(ctx, logger)
		})
	}
}

// TestClientSubsetInjection checks any subset of sdk.Client is injected, even
// an unnamed one.
func (s *TaskSuite) TestClientSubsetInjection() {
	task, err := NewTaskFunction(func(client interface {
		GetVariable(ctx context.Context, key string) (string, error)
	},
	) error {
		s.NotNil(client)
		return nil
	})
	s.Require().NoError(err)
	s.Require().NoError(task.Execute(context.Background(), slog.New(logging.NewTeeLogger())))
}

// TestNamedClientInterfacesAreInjectable guards against sdk.Client dropping an
// embedded interface, which would break tasks declaring it.
func (s *TaskSuite) TestNamedClientInterfacesAreInjectable() {
	for name, typ := range map[string]reflect.Type{
		"Client":           reflect.TypeFor[sdk.Client](),
		"VariableClient":   reflect.TypeFor[sdk.VariableClient](),
		"ConnectionClient": reflect.TypeFor[sdk.ConnectionClient](),
		"XComClient":       reflect.TypeFor[sdk.XComClient](),
	} {
		s.True(isClient(typ), "sdk.%s must stay injectable", name)
	}
}

// TestNonInjectableParamsAreRejected checks registration fails fast on
// interface parameters Execute cannot inject.
func (s *TaskSuite) TestNonInjectableParamsAreRejected() {
	cases := map[string]struct {
		fn          any
		errContains string
	}{
		"non-client-method": {
			func(x interface{ NotAClientMethod() }) error { return nil },
			"sdk.Client has no method NotAClientMethod",
		},
		"wrong-signature": {
			func(x interface {
				GetVariable(key string) (string, error)
			},
			) error {
				return nil
			},
			"method GetVariable is func(context.Context, string) (string, error) on sdk.Client",
		},
		"context-with-extra-methods": {
			func(x interface {
				context.Context
				TaskInstance() sdk.TaskInstance
			},
			) error {
				return nil
			},
			"adds methods on top of context.Context",
		},
	}

	for name, tt := range cases {
		s.Run(name, func() {
			_, err := NewTaskFunction(tt.fn)
			if s.Assert().Error(err) {
				s.Assert().Contains(err.Error(), "parameter 0")
				s.Assert().Contains(err.Error(), tt.errContains)
			}
		})
	}
}

// probeKey is an unexported context key used to confirm the live task context
// (not a freshly built one) backs the injected sdk.TIRunContext.
type probeKeyType struct{}

var probeKey probeKeyType

// TestTIRunContextInjection verifies a task declaring sdk.TIRunContext receives
// the TaskInstance/DagRun stored on the context, backed by the live task
// context so it is usable as a context.Context. It must take precedence over
// the plain context.Context binding, which sdk.TIRunContext also satisfies.
func (s *TaskSuite) TestTIRunContextInjection() {
	mapIndex := 3
	ti := sdk.TaskInstance{
		DagID:     "dag1",
		RunID:     "run1",
		TaskID:    "task1",
		MapIndex:  &mapIndex,
		TryNumber: 2,
	}
	dagRun := sdk.DagRun{DagID: "dag1", RunID: "run1"}
	stored := sdk.NewTIRunContext(context.Background(), ti, dagRun)

	var got sdk.TIRunContext
	task, err := NewTaskFunction(func(ctx sdk.TIRunContext) error {
		got = ctx
		return nil
	})
	s.Require().NoError(err)

	ctx := context.WithValue(context.Background(), sdkcontext.RuntimeContextKey, stored)
	ctx = context.WithValue(ctx, probeKey, "probe-value")
	s.Require().NoError(task.Execute(ctx, slog.New(logging.NewTeeLogger())))

	s.Require().NotNil(got, "the task must receive a non-nil TIRunContext")
	s.Equal(ti, got.TaskInstance())
	s.Equal(dagRun, got.DagRun())
	s.Equal(
		"probe-value",
		got.Value(probeKey),
		"the injected context must be backed by the one passed to Execute",
	)
}

// TestTIRunContextInjectionWithoutRuntimeContext covers the Edge Worker path:
// the runtime does not populate RuntimeContextKey, so a task declaring
// sdk.TIRunContext gets zero TaskInstance/DagRun but is still backed by the
// live task context, leaving it usable as a context.Context.
func (s *TaskSuite) TestTIRunContextInjectionWithoutRuntimeContext() {
	var got sdk.TIRunContext
	task, err := NewTaskFunction(func(ctx sdk.TIRunContext) error {
		got = ctx
		return nil
	})
	s.Require().NoError(err)

	ctx := context.WithValue(context.Background(), probeKey, "probe-value")
	s.Require().NoError(task.Execute(ctx, slog.New(logging.NewTeeLogger())))

	s.Require().NotNil(got, "the task must receive a non-nil TIRunContext")
	s.Equal(
		sdk.TaskInstance{},
		got.TaskInstance(),
		"TaskInstance must be zero when no runtime context is present",
	)
	s.Equal(
		sdk.DagRun{},
		got.DagRun(),
		"DagRun must be zero when no runtime context is present",
	)
	s.Equal(
		"probe-value",
		got.Value(probeKey),
		"the injected context must be backed by the one passed to Execute",
	)
}

// --- Data-parameter (TaskFlow Inputs) binding ---

// fakeClient implements sdk.Client with canned per-task XCom values, recording
// the pulls Execute performs to resolve data parameters.
type fakeClient struct {
	xcoms   map[string]any // upstream task id -> stored return_value
	getErr  error
	pulls   []string
	pushed  map[string]any
	pushErr error
}

func (f *fakeClient) GetVariable(context.Context, string) (string, error) { return "", nil }
func (f *fakeClient) UnmarshalJSONVariable(context.Context, string, any) error {
	return nil
}

func (f *fakeClient) GetConnection(context.Context, string) (sdk.Connection, error) {
	return sdk.Connection{}, nil
}

func (f *fakeClient) GetXCom(
	_ context.Context,
	dagId, runId, taskId string,
	mapIndex *int,
	key string,
	_ any,
) (any, error) {
	f.pulls = append(f.pulls, taskId)
	if f.getErr != nil {
		return nil, f.getErr
	}
	if dagId != "dag1" || runId != "run1" || key != api.XComReturnValueKey || mapIndex != nil {
		return nil, errors.New("unexpected xcom pull identifiers")
	}
	return f.xcoms[taskId], nil
}

func (f *fakeClient) PushXCom(_ context.Context, _ api.TaskInstance, key string, value any) error {
	if f.pushed == nil {
		f.pushed = map[string]any{}
	}
	f.pushed[key] = value
	return f.pushErr
}

// bindingResult is the struct shape the fake upstream produces.
type bindingResult struct {
	Value int    `json:"value"`
	Name  string `json:"name"`
}

func (s *TaskSuite) executionContext(client sdk.Client) context.Context {
	ctx := context.WithValue(context.Background(), sdkcontext.SdkClientContextKey, client)
	return context.WithValue(ctx, sdkcontext.WorkloadContextKey, api.ExecuteTaskWorkload{
		TI: api.TaskInstance{DagId: "dag1", RunId: "run1", TaskId: "consumer"},
	})
}

func (s *TaskSuite) TestDataParamInjection() {
	client := &fakeClient{xcoms: map[string]any{
		"extract": map[string]any{"value": 42, "name": "answer"},
	}}

	var got bindingResult
	task, err := NewTaskFunction(func(in bindingResult) error {
		got = in
		return nil
	}, InputBinding{TaskID: "extract"})
	s.Require().NoError(err)

	err = task.Execute(s.executionContext(client), slog.New(logging.NewTeeLogger()))
	s.Require().NoError(err)
	s.Equal(bindingResult{Value: 42, Name: "answer"}, got)
	s.Equal([]string{"extract"}, client.pulls)
}

func (s *TaskSuite) TestDataParamsBindPositionallyAmongInjectables() {
	client := &fakeClient{xcoms: map[string]any{
		"first":  map[string]any{"value": 1, "name": "a"},
		"second": map[string]any{"value": 2, "name": "b"},
	}}

	var gotA, gotB bindingResult
	task, err := NewTaskFunction(
		func(ctx context.Context, a bindingResult, logger *slog.Logger, b bindingResult, c sdk.XComClient) error {
			s.NotNil(ctx)
			s.NotNil(logger)
			s.NotNil(c)
			gotA, gotB = a, b
			return nil
		},
		InputBinding{TaskID: "first"},
		InputBinding{TaskID: "second"},
	)
	s.Require().NoError(err)

	err = task.Execute(s.executionContext(client), slog.New(logging.NewTeeLogger()))
	s.Require().NoError(err)
	s.Equal(bindingResult{Value: 1, Name: "a"}, gotA)
	s.Equal(bindingResult{Value: 2, Name: "b"}, gotB)
	s.Equal([]string{"first", "second"}, client.pulls)
}

func (s *TaskSuite) TestDataParamStrictDecodeRejectsUnknownKeys() {
	client := &fakeClient{xcoms: map[string]any{
		"extract": map[string]any{"value": 1, "renamed_field": true},
	}}

	task, err := NewTaskFunction(
		func(in bindingResult) error { return nil },
		InputBinding{TaskID: "extract"},
	)
	s.Require().NoError(err)

	err = task.Execute(s.executionContext(client), slog.New(logging.NewTeeLogger()))
	if s.Error(err) {
		s.Contains(err.Error(), `decoding input from task "extract"`)
		s.Contains(err.Error(), "renamed_field")
	}
}

func (s *TaskSuite) TestDataParamLooseMapDecodesAnyShape() {
	client := &fakeClient{xcoms: map[string]any{
		"extract": map[string]any{"anything": "goes", "extra": 1},
	}}

	var got map[string]any
	task, err := NewTaskFunction(
		func(in map[string]any) error {
			got = in
			return nil
		},
		InputBinding{TaskID: "extract"},
	)
	s.Require().NoError(err)

	s.Require().NoError(task.Execute(s.executionContext(client), slog.New(logging.NewTeeLogger())))
	s.Equal("goes", got["anything"])
}

func (s *TaskSuite) TestDataParamNullValue() {
	client := &fakeClient{xcoms: map[string]any{"extract": nil}}

	s.Run("nilable-target-gets-zero", func() {
		var got *bindingResult
		task, err := NewTaskFunction(
			func(in *bindingResult) error {
				got = in
				return nil
			},
			InputBinding{TaskID: "extract"},
		)
		s.Require().NoError(err)
		s.Require().
			NoError(task.Execute(s.executionContext(client), slog.New(logging.NewTeeLogger())))
		s.Nil(got)
	})

	s.Run("non-nilable-target-fails", func() {
		task, err := NewTaskFunction(
			func(in bindingResult) error { return nil },
			InputBinding{TaskID: "extract"},
		)
		s.Require().NoError(err)
		err = task.Execute(s.executionContext(client), slog.New(logging.NewTeeLogger()))
		if s.Error(err) {
			s.Contains(err.Error(), "not nilable")
		}
	})
}

func (s *TaskSuite) TestDataParamPullFailureFailsTask() {
	client := &fakeClient{getErr: errors.New("supervisor unreachable")}

	ran := false
	task, err := NewTaskFunction(
		func(in bindingResult) error {
			ran = true
			return nil
		},
		InputBinding{TaskID: "extract"},
	)
	s.Require().NoError(err)

	err = task.Execute(s.executionContext(client), slog.New(logging.NewTeeLogger()))
	if s.Error(err) {
		s.Contains(err.Error(), `pulling input from task "extract"`)
	}
	s.False(ran, "the task body must not run when an input pull fails")
}

func (s *TaskSuite) TestDataParamWithoutWorkloadFails() {
	client := &fakeClient{xcoms: map[string]any{"extract": map[string]any{"value": 1}}}
	task, err := NewTaskFunction(
		func(in map[string]any) error { return nil },
		InputBinding{TaskID: "extract"},
	)
	s.Require().NoError(err)

	// Client injected but no workload on the context.
	ctx := context.WithValue(
		context.Background(),
		sdkcontext.SdkClientContextKey,
		sdk.Client(client),
	)
	err = task.Execute(ctx, slog.New(logging.NewTeeLogger()))
	if s.Error(err) {
		s.Contains(err.Error(), "no workload in context")
	}
}

func (s *TaskSuite) TestInputBindingValidation() {
	s.Run("count-mismatch", func() {
		_, err := NewTaskFunction(func(x any) error { return nil })
		if s.Error(err) {
			s.Contains(
				err.Error(),
				"declares 1 data parameter(s) but 0 input binding(s) were supplied",
			)
		}
	})

	s.Run("empty-task-id", func() {
		_, err := NewTaskFunction(
			func(x any) error { return nil },
			InputBinding{},
		)
		if s.Error(err) {
			s.Contains(err.Error(), "input binding 0 has an empty task id")
		}
	})

	s.Run("undecodable-param", func() {
		_, err := NewTaskFunction(
			func(x chan int) error { return nil },
			InputBinding{TaskID: "extract"},
		)
		if s.Error(err) {
			s.Contains(err.Error(), "cannot hold an XCom value")
		}
	})
}
