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

package bundle

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/apache/airflow/go-sdk/internal/contexttest"
	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/logging"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

type TaskSuite struct {
	suite.Suite
}

type taskTestClient struct {
	sdk.Client
}

func withTaskClient(ctx context.Context) context.Context {
	return context.WithValue(ctx, sdkcontext.SdkClientContextKey, sdk.Client(&taskTestClient{}))
}

func init() { binding.RegisterTaskContext(contexttest.New) }

func TestTaskSuite(t *testing.T) {
	suite.Run(t, &TaskSuite{})
}

func (s *TaskSuite) TestReturnValidation() {
	cases := map[string]struct {
		fn          any
		errContains string
	}{
		"no-ret-values": {
			func(contexttest.Context) {},
			`func\d+ has 0 return values, must be`,
		},
		"too-many-ret-values": {
			func(contexttest.Context) (a, b, c int) { return },
			`func\d+ has 3 return values, must be`,
		},
		"invalid-ret": {
			func(contexttest.Context) (c chan int) { return },
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

func (s *TaskSuite) TestRejectsNonFunc() {
	cases := map[string]struct {
		fn      any
		wantErr string
	}{
		"int":     {3, "expected a func as input but was int"},
		"nil":     {nil, "expected a func as input but was invalid"},
		"pointer": {new(int), "expected a func as input but was ptr"},
	}

	for name, tt := range cases {
		s.Run(name, func() {
			_, err := NewTaskFunction(tt.fn)
			s.Assert().EqualError(err, tt.wantErr)
		})
	}
}

// probeKey is an unexported context key used to confirm the live task context
// (not a freshly built one) backs the airflow.Context a task receives.
type probeKeyType struct{}

var probeKey probeKeyType

func (s *TaskSuite) TestExecuteBindsAirflowContext() {
	mapIndex := 3
	ti := sdk.TaskInstance{
		DagID:     "dag1",
		RunID:     "run1",
		TaskID:    "task1",
		MapIndex:  &mapIndex,
		TryNumber: 2,
	}
	dagRun := sdk.DagRun{DagID: "dag1", RunID: "run1"}

	var got contexttest.Context
	task, err := NewTaskFunction(func(actx contexttest.Context) error {
		got = actx
		return nil
	})
	s.Require().NoError(err)

	ctx := context.WithValue(
		withTaskClient(context.Background()),
		sdkcontext.RuntimeContextKey,
		sdk.NewTIRunContext(context.Background(), ti, dagRun),
	)
	ctx = context.WithValue(ctx, probeKey, "probe-value")
	logger := slog.New(logging.NewTeeLogger())
	s.Require().NoError(task.Execute(ctx, logger, nil))

	s.Same(logger, got.Logger())
	s.Equal(ctx.Value(sdkcontext.SdkClientContextKey), got.Client())
	s.Equal(ti, got.TaskInstance())
	s.Equal(dagRun, got.DagRun())
	s.Equal(
		"probe-value",
		got.Value(probeKey),
		"the Context must be backed by the one passed to Execute",
	)
}

func (s *TaskSuite) TestExecuteBindsDataParameters() {
	var gotCountry string
	var gotMeta map[string]any
	task, err := NewTaskFunction(
		func(actx contexttest.Context, country string, meta map[string]any) error {
			gotCountry = country
			gotMeta = meta
			return nil
		},
	)
	s.Require().NoError(err)

	err = task.Execute(
		withTaskClient(context.Background()),
		slog.New(logging.NewTeeLogger()),
		[]binding.Arg{
			binding.LiteralArg{Value: "uk"},
			binding.LiteralArg{Value: map[string]any{"k": "v"}},
		},
	)
	s.Require().NoError(err)
	s.Equal("uk", gotCountry)
	s.Equal(map[string]any{"k": "v"}, gotMeta)
}

func (s *TaskSuite) TestExecuteWithoutSpecFailsForDataParameters() {
	task, err := NewTaskFunction(
		func(actx contexttest.Context, country string) error { return nil },
	)
	s.Require().NoError(err)

	err = task.Execute(
		withTaskClient(context.Background()), slog.New(logging.NewTeeLogger()), nil,
	)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "argument count mismatch")
	}
}

func (s *TaskSuite) TestExecuteArityMismatch() {
	task, err := NewTaskFunction(
		func(actx contexttest.Context, country string) error { return nil },
	)
	s.Require().NoError(err)

	err = task.Execute(
		withTaskClient(context.Background()),
		slog.New(logging.NewTeeLogger()),
		[]binding.Arg{
			binding.LiteralArg{Value: "uk"},
			binding.LiteralArg{Value: "de"},
		},
	)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "argument count mismatch")
		s.Contains(err.Error(), "passes 2 positional argument(s)")
	}
}

func (s *TaskSuite) TestExecuteRequiresCoordinatorClient() {
	task, err := NewTaskFunction(func(contexttest.Context) error { return nil })
	s.Require().NoError(err)

	err = task.Execute(context.Background(), slog.New(logging.NewTeeLogger()), nil)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "coordinator SDK client is missing")
	}
}
