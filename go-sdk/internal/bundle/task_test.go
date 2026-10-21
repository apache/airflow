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
	"errors"
	"fmt"
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

type taskError struct{}

func (*taskError) Error() string { return "task error" }

type errno int

func (errno) Error() string { return "errno" }

type codedError interface {
	error
	Code() int
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
		"pointer-error-ret": {
			func(contexttest.Context) (int, *taskError) { return 0, nil },
			`func\d+ must declare its last result as error, not \*bundle\.taskError$`,
		},
		"value-error-ret": {
			func(contexttest.Context) errno { return 0 },
			`func\d+ must declare its last result as error, not bundle\.errno$`,
		},
		"interface-error-ret": {
			func(contexttest.Context) (int, codedError) { return 0, nil },
			`func\d+ must declare its last result as error, not bundle\.codedError$`,
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

// branchClient records each XCom that a task pushes and keeps the last value of each key.
// PushXCom fails for the call whose record equals failOn. Its skip method stands in for the
// function that the runtime passes through WithSkipDownstreamTasks. skip records each call in the
// same list as the XComs, so that the tests see the order of the calls. It returns skipErr.
type branchClient struct {
	sdk.Client
	calls   []string
	values  map[string]any
	failOn  string
	skipErr error
}

func (c *branchClient) PushXCom(
	_ context.Context,
	ti sdk.TaskInstance,
	key string,
	value any,
) error {
	call := fmt.Sprintf("PushXCom %s %s %v", ti.TaskID, key, value)
	c.calls = append(c.calls, call)
	if c.values == nil {
		c.values = map[string]any{}
	}
	c.values[key] = value
	if call == c.failOn {
		return errors.New("xcom refused")
	}
	return nil
}

func (c *branchClient) skip(_ context.Context, taskIDs []string) error {
	c.calls = append(c.calls, fmt.Sprintf("SkipDownstreamTasks %v", taskIDs))
	return c.skipErr
}

// runBranch runs task as the task instance decide of dag1 with client. When ti is false, the
// context has no task instance. When canSkip is false, the context has no function to skip
// tasks with.
func runBranch(task Task, client *branchClient, ti, canSkip bool) error {
	ctx := context.WithValue(
		context.Background(),
		sdkcontext.SdkClientContextKey,
		sdk.Client(client),
	)
	if ti {
		ctx = context.WithValue(ctx, sdkcontext.RuntimeContextKey, sdk.NewTIRunContext(
			context.Background(),
			sdk.TaskInstance{DagID: "dag1", RunID: "run1", TaskID: "decide"},
			sdk.DagRun{DagID: "dag1", RunID: "run1"},
		))
	}
	if canSkip {
		ctx = WithSkipDownstreamTasks(ctx, client.skip)
	}
	return task.Execute(ctx, slog.New(logging.NewTeeLogger()), nil)
}

// The task pushes the value that decide returns, not the result of fn.
func (s *TaskSuite) TestBranchFunctionPushesTheValueAndSkipsTheTasksThatDecideReturns() {
	var got any
	task, err := NewPositionalBranchFunction(
		func(contexttest.Context) (bool, error) { return true, nil },
		func(result any) (any, []string, error) {
			got = result
			return "chosen", []string{"load", "report"}, nil
		},
	)
	s.Require().NoError(err)

	client := &branchClient{}
	s.Require().NoError(runBranch(task, client, true, true))

	s.Equal(true, got)
	s.Equal([]string{
		"PushXCom decide return_value chosen",
		"PushXCom decide skipmixin_key map[skipped:[load report]]",
		"SkipDownstreamTasks [load report]",
	}, client.calls)
}

func (s *TaskSuite) TestBranchFunctionPushesNoNilPointer() {
	task, err := NewPositionalBranchFunction(
		func(contexttest.Context) (bool, error) { return true, nil },
		func(any) (any, []string, error) { return (*int)(nil), []string{"load"}, nil },
	)
	s.Require().NoError(err)

	client := &branchClient{}
	s.Require().NoError(runBranch(task, client, true, true))

	s.Equal([]string{
		"PushXCom decide skipmixin_key map[skipped:[load]]",
		"SkipDownstreamTasks [load]",
	}, client.calls)
}

func (s *TaskSuite) TestBranchFunctionWithNothingToSkip() {
	for name, skipped := range map[string][]string{"nil": nil, "empty": {}} {
		s.Run(name, func() {
			task, err := NewPositionalBranchFunction(
				func(contexttest.Context) (bool, error) { return true, nil },
				func(result any) (any, []string, error) { return result, skipped, nil },
			)
			s.Require().NoError(err)

			client := &branchClient{}
			s.Require().NoError(runBranch(task, client, true, true))

			s.Equal([]string{"PushXCom decide return_value true"}, client.calls)
		})
	}
}

// A try whose fn fails pushes no XCom and skips nothing, whether fn returns an error or panics.
func (s *TaskSuite) TestBranchFunctionThatFailsPushesAndSkipsNothing() {
	cases := map[string]func(contexttest.Context) (bool, error){
		"error": func(contexttest.Context) (bool, error) { return false, errors.New("no table") },
		"panic": func(contexttest.Context) (bool, error) { panic("no table") },
	}
	for name, fn := range cases {
		s.Run(name, func() {
			called := false
			task, err := NewPositionalBranchFunction(fn, func(result any) (any, []string, error) {
				called = true
				return result, []string{"load"}, nil
			})
			s.Require().NoError(err)

			client := &branchClient{}
			func() {
				defer func() { _ = recover() }()
				s.Error(runBranch(task, client, true, true))
			}()

			s.False(called)
			s.Empty(client.calls)
		})
	}
}

func (s *TaskSuite) TestBranchFunctionFailsWhenDecideReturnsAnError() {
	task, err := NewPositionalBranchFunction(
		func(contexttest.Context) (bool, error) { return true, nil },
		func(any) (any, []string, error) {
			return "chosen", []string{"load"}, errors.New("not one of the cases")
		},
	)
	s.Require().NoError(err)

	client := &branchClient{}
	s.EqualError(runBranch(task, client, true, true), "not one of the cases")
	s.Empty(client.calls)
}

func (s *TaskSuite) TestBranchFunctionFailsWhenItCannotSkip() {
	cases := map[string]struct {
		client    *branchClient
		withTI    bool
		canSkip   bool
		wantErr   string
		wantRun   bool
		wantCalls []string
	}{
		"runtime cannot skip": {
			client:  &branchClient{},
			withTI:  true,
			wantErr: "the task runtime cannot skip downstream tasks",
		},
		"no task instance": {
			client:  &branchClient{},
			canSkip: true,
			wantErr: "task runtime context is missing",
		},
		"recording the skipped tasks fails": {
			client: &branchClient{
				failOn: "PushXCom decide skipmixin_key map[skipped:[load]]",
			},
			withTI:  true,
			canSkip: true,
			wantErr: "recording the skipped tasks in the skipmixin_key XCom: xcom refused",
			wantRun: true,
			wantCalls: []string{
				"PushXCom decide return_value false",
				"PushXCom decide skipmixin_key map[skipped:[load]]",
			},
		},
		"skip fails": {
			client:  &branchClient{skipErr: errors.New("supervisor refused")},
			withTI:  true,
			canSkip: true,
			wantErr: `skipping the downstream tasks ["load"]: supervisor refused`,
			wantRun: true,
			wantCalls: []string{
				"PushXCom decide return_value false",
				"PushXCom decide skipmixin_key map[skipped:[load]]",
				"SkipDownstreamTasks [load]",
			},
		},
	}
	for name, tt := range cases {
		s.Run(name, func() {
			ran := false
			task, err := NewPositionalBranchFunction(
				func(contexttest.Context) (bool, error) {
					ran = true
					return false, nil
				},
				func(result any) (any, []string, error) { return result, []string{"load"}, nil },
			)
			s.Require().NoError(err)

			s.EqualError(runBranch(task, tt.client, tt.withTI, tt.canSkip), tt.wantErr)
			s.Equal(tt.wantRun, ran, "whether fn ran")
			s.Equal(tt.wantCalls, tt.client.calls)
		})
	}
}
