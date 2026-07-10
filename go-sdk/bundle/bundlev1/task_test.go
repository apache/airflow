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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/apache/airflow/go-sdk/pkg/binding"
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

// TestNonInjectableParamsAreRejected checks registration fails fast on
// parameters Execute can neither inject nor bind a task argument to. This
// replaces the historical silent zero-fill of unrecognized parameters.
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
		"func-param": {
			func(cb func()) error { return nil },
			"cannot receive a task argument",
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

// TestExecuteArgsBindsDataParameters covers the TaskFlow path end to end at the
// task level: literals decode onto data parameters interleaved with
// injectables, and Execute (nil args) keeps working for argless functions.
func (s *TaskSuite) TestExecuteArgsBindsDataParameters() {
	var gotCountry string
	var gotMeta map[string]any
	task, err := NewTaskFunction(func(log *slog.Logger, country string, meta map[string]any) error {
		gotCountry = country
		gotMeta = meta
		return nil
	})
	s.Require().NoError(err)

	tw, ok := task.(TaskWithArgs)
	s.Require().True(ok, "taskFunction must implement TaskWithArgs")

	err = tw.ExecuteArgs(context.Background(), slog.New(logging.NewTeeLogger()), []binding.Arg{
		{Kind: binding.ArgKindLiteral, Value: "uk", DataType: binding.DataTypeString},
		{
			Kind:     binding.ArgKindLiteral,
			Value:    map[string]any{"k": "v"},
			DataType: binding.DataTypeObject,
		},
	})
	s.Require().NoError(err)
	s.Equal("uk", gotCountry)
	s.Equal(map[string]any{"k": "v"}, gotMeta)
}

// TestExecuteWithoutArgsFailsForDataParameters: a function with data
// parameters run through the argless Execute path (e.g. the Edge Worker, or a
// stub Dag that passes no arguments) fails loudly on the arity check instead
// of silently zero-filling.
func (s *TaskSuite) TestExecuteWithoutArgsFailsForDataParameters() {
	task, err := NewTaskFunction(func(country string) error { return nil })
	s.Require().NoError(err)

	err = task.Execute(context.Background(), slog.New(logging.NewTeeLogger()))
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "argument count mismatch")
	}
}

// TestExecuteArgsArityMismatch fails loudly when the Dag passes more arguments
// than the function declares data parameters.
func (s *TaskSuite) TestExecuteArgsArityMismatch() {
	task, err := NewTaskFunction(func(country string) error { return nil })
	s.Require().NoError(err)

	err = task.(TaskWithArgs).ExecuteArgs(
		context.Background(),
		slog.New(logging.NewTeeLogger()),
		[]binding.Arg{
			{Kind: binding.ArgKindLiteral, Value: "uk"},
			{Kind: binding.ArgKindLiteral, Value: "de"},
		},
	)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "argument count mismatch")
		s.Contains(err.Error(), "passes 2 positional argument(s)")
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
