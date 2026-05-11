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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/apache/airflow/go-sdk/sdk"
)

func myTask() error { return nil }
func myTaskWithArgs(ctx context.Context, logger *slog.Logger, client sdk.Client) error {
	if ctx == nil || logger == nil || client == nil {
		return errors.New("missing required argument")
	}
	return nil
}
func errorTask() error { return errors.New("fail") }

func NotErrorRet() int {
	return 0
}

type RegistrySuite struct {
	suite.Suite
	reg Registry
	dag Dag
}

func TestRegistrySuite(t *testing.T) {
	suite.Run(t, &RegistrySuite{})
}

func (s *RegistrySuite) SetupTest() {
	s.reg = New()
	s.dag = s.reg.AddDag("dag1")
}

func (s *RegistrySuite) TestAddDag_NewDagRegisters() {
	s.NotNil(s.dag)
}

func (s *RegistrySuite) TestAddDag_DuplicatePanics() {
	s.PanicsWithError(`Dag "dag1" already exists in bundle`, func() {
		s.reg.AddDag("dag1")
	})
}

func (s *RegistrySuite) TestAddTask_RegistersAndFindsTask() {
	s.dag.AddTask(myTask)
	task, exists := s.reg.LookupTask("dag1", "myTask")
	s.True(exists)
	s.NotNil(task)
}

func (s *RegistrySuite) TestAddTaskWithName_RegistersAndFindsTask() {
	s.dag.AddTaskWithName("special", myTask)
	task, exists := s.reg.LookupTask("dag1", "special")
	s.True(exists)
	s.NotNil(task)

	// Lets just make sure it didn't exist under the fn name
	_, exists = s.reg.LookupTask("dag1", "myTask")
	s.False(exists)
}

func (s *RegistrySuite) TestRegisterTaskWithName_DuplicatePanics() {
	s.dag.AddTaskWithName("special", myTask)
	s.PanicsWithError("taskId \"special\" is already registered for DAG \"dag1\"", func() {
		s.dag.AddTaskWithName("special", myTask)
	})
}

func (s *RegistrySuite) TestAddTask_NonFuncPanics() {
	s.PanicsWithError("task fn was a string, not a func", func() {
		s.dag.AddTask("not a func")
	})
}

func (s *RegistrySuite) TestAddTaskWithArgs_BindsCorrectArgs() {
	s.dag.AddTask(myTaskWithArgs)
	task, exists := s.reg.LookupTask("dag1", "myTaskWithArgs")
	s.True(exists)
	s.NotNil(task)
}

func (s *RegistrySuite) TestAddTask_InvalidReturnType() {
	s.PanicsWithError(
		"error registering task \"NotErrorRet\" for DAG \"dag1\": expected task function github.com/apache/airflow/go-sdk/bundle/bundlev1.NotErrorRet last return value to return error but found int",
		func() {
			s.dag.AddTask(NotErrorRet)
		},
	)
}

func (s *RegistrySuite) TestAddTask_ErrorReturnType() {
	s.dag.AddTask(errorTask)
	_, exists := s.reg.LookupTask("dag1", "errorTask")
	s.True(exists)
}

func (s *RegistrySuite) TestAddTask_WithSpec() {
	s.dag.AddTask(myTask, TaskSpec{Queue: "high_mem", Retries: 3, DoXComPush: Bool(false)})
	enum, ok := s.reg.(EnumerableBundle)
	s.Require().True(ok)
	dags := enum.OrderedDags()
	s.Require().Len(dags, 1)
	s.Require().Len(dags[0].Tasks, 1)
	got := dags[0].Tasks[0]
	s.Equal("myTask", got.ID)
	s.Equal("high_mem", got.Spec.Queue)
	s.Equal(3, got.Spec.Retries)
	s.Require().NotNil(got.Spec.DoXComPush)
	s.False(*got.Spec.DoXComPush)
}

func (s *RegistrySuite) TestAddTaskWithName_WithSpec() {
	s.dag.AddTaskWithName("special", myTask, TaskSpec{Queue: "gpu", Pool: "gpu_pool"})
	enum, ok := s.reg.(EnumerableBundle)
	s.Require().True(ok)
	dags := enum.OrderedDags()
	s.Require().Len(dags, 1)
	s.Require().Len(dags[0].Tasks, 1)
	got := dags[0].Tasks[0]
	s.Equal("special", got.ID)
	s.Equal("gpu", got.Spec.Queue)
	s.Equal("gpu_pool", got.Spec.Pool)
}

func (s *RegistrySuite) TestAddTask_TooManySpecsPanics() {
	s.PanicsWithError("AddTask accepts at most one spec, got 2", func() {
		s.dag.AddTask(myTask, TaskSpec{}, TaskSpec{})
	})
}

func (s *RegistrySuite) TestAddDag_WithSpec() {
	dag2 := s.reg.AddDag(
		"dag2",
		DagSpec{Schedule: "@daily", Tags: []string{"etl"}, MaxActiveRuns: 4},
	)
	s.NotNil(dag2)
	enum, ok := s.reg.(EnumerableBundle)
	s.Require().True(ok)
	dags := enum.OrderedDags()
	s.Require().Len(dags, 2)
	var got DagInfo
	for _, d := range dags {
		if d.DagID == "dag2" {
			got = d
			break
		}
	}
	s.Equal("dag2", got.DagID)
	s.Equal("@daily", got.Spec.Schedule)
	s.Equal([]string{"etl"}, got.Spec.Tags)
	s.Equal(4, got.Spec.MaxActiveRuns)
}

func (s *RegistrySuite) TestAddDag_TooManySpecsPanics() {
	s.PanicsWithError("AddDag accepts at most one spec, got 2", func() {
		s.reg.AddDag("dag3", DagSpec{}, DagSpec{})
	})
}
