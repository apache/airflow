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

// producer / consumer fixtures for Inputs wiring.
type payload struct {
	Value int `json:"value"`
}

func produce() (payload, error)             { return payload{Value: 1}, nil }
func consume(in payload) error              { _ = in; return nil }
func relay(in payload) (payload, error)     { return in, nil }
func consumeTwo(a payload, b payload) error { _, _ = a, b; return nil }
func consumeLoose(in map[string]any) error  { _ = in; return nil }
func consumeString(in string) error         { _ = in; return nil }

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
	s.dag = s.reg.AddDag(DagSpec{DagId: "dag1"})
}

func (s *RegistrySuite) TestAddDag_NewDagRegisters() {
	s.NotNil(s.dag)
}

func (s *RegistrySuite) TestAddDag_MissingIdPanics() {
	s.PanicsWithError("AddDag requires DagSpec.DagId to be set", func() {
		s.reg.AddDag(DagSpec{})
	})
}

func (s *RegistrySuite) TestAddDag_DuplicatePanics() {
	s.PanicsWithError(`Dag "dag1" already exists in bundle`, func() {
		s.reg.AddDag(DagSpec{DagId: "dag1"})
	})
}

func (s *RegistrySuite) TestTask_RegistersAndFindsTask() {
	ref := s.dag.Task(myTask)
	s.Equal("myTask", ref.ID())
	task, exists := s.reg.LookupTask("dag1", "myTask")
	s.True(exists)
	s.NotNil(task)
}

func (s *RegistrySuite) TestTask_ExplicitIdViaSpec() {
	ref := s.dag.Task(myTask, TaskSpec{TaskId: "special"})
	s.Equal("special", ref.ID())
	_, exists := s.reg.LookupTask("dag1", "special")
	s.True(exists)

	// Lets just make sure it didn't exist under the fn name
	_, exists = s.reg.LookupTask("dag1", "myTask")
	s.False(exists)
}

func (s *RegistrySuite) TestTask_DuplicateIdPanics() {
	s.dag.Task(myTask, TaskSpec{TaskId: "special"})
	s.PanicsWithError("taskId \"special\" is already registered for DAG \"dag1\"", func() {
		s.dag.Task(myTask, TaskSpec{TaskId: "special"})
	})
}

func (s *RegistrySuite) TestTask_NonFuncPanics() {
	s.PanicsWithError("task fn was a string, not a func", func() {
		s.dag.Task("not a func")
	})
}

func (s *RegistrySuite) TestTask_InjectedArgsBind() {
	s.dag.Task(myTaskWithArgs)
	task, exists := s.reg.LookupTask("dag1", "myTaskWithArgs")
	s.True(exists)
	s.NotNil(task)
}

func (s *RegistrySuite) TestTask_InvalidReturnType() {
	s.PanicsWithError(
		"error registering task \"NotErrorRet\" for DAG \"dag1\": expected task function github.com/apache/airflow/go-sdk/bundle/bundlev1.NotErrorRet last return value to return error but found int",
		func() {
			s.dag.Task(NotErrorRet)
		},
	)
}

func (s *RegistrySuite) TestTask_ErrorReturnType() {
	s.dag.Task(errorTask)
	_, exists := s.reg.LookupTask("dag1", "errorTask")
	s.True(exists)
}

func (s *RegistrySuite) TestTask_TooManySpecsPanics() {
	s.PanicsWithError("Task accepts at most one TaskSpec", func() {
		s.dag.Task(myTask, TaskSpec{}, TaskSpec{})
	})
}

func (s *RegistrySuite) TestOrderedDags_Empty() {
	enum, ok := New().(EnumerableBundle)
	s.Require().True(ok)
	s.Empty(enum.OrderedDags())
}

func (s *RegistrySuite) TestOrderedDags_PreservesRegistrationOrder() {
	reg := New()
	// Register dags out of alphabetical order so the test fails if OrderedDags
	// ever sorts instead of preserving AddDag order.
	zeta := reg.AddDag(DagSpec{DagId: "zeta"})
	zeta.Task(myTask, TaskSpec{TaskId: "z1"})
	zeta.Task(myTask, TaskSpec{TaskId: "z2"})

	alpha := reg.AddDag(DagSpec{DagId: "alpha"})
	alpha.Task(myTask, TaskSpec{TaskId: "a1"})

	reg.AddDag(DagSpec{DagId: "mid"}) // dag with no tasks

	enum, ok := reg.(EnumerableBundle)
	s.Require().True(ok)

	got := enum.OrderedDags()
	s.Require().Len(got, 3)

	taskIDs := func(tasks []TaskInfo) []string {
		ids := make([]string, 0, len(tasks))
		for _, t := range tasks {
			ids = append(ids, t.ID)
		}
		return ids
	}

	s.Equal("zeta", got[0].DagID)
	s.Equal([]string{"z1", "z2"}, taskIDs(got[0].Tasks))

	s.Equal("alpha", got[1].DagID)
	s.Equal([]string{"a1"}, taskIDs(got[1].Tasks))

	s.Equal("mid", got[2].DagID)
	s.Empty(got[2].Tasks)
}

func (s *RegistrySuite) TestTask_WithSpec() {
	s.dag.Task(myTask, TaskSpec{Queue: "high_mem", Retries: 3, DoXComPush: Bool(false)})
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

func (s *RegistrySuite) infoByID() map[string]TaskInfo {
	enum := s.reg.(EnumerableBundle)
	tasks := enum.OrderedDags()[0].Tasks
	byID := make(map[string]TaskInfo, len(tasks))
	for _, t := range tasks {
		byID[t.ID] = t
	}
	return byID
}

func (s *RegistrySuite) TestTask_InputsRecordEdgesAndBindings() {
	extract := s.dag.Task(produce, TaskSpec{TaskId: "extract"})
	transform := s.dag.Task(relay, TaskSpec{TaskId: "transform"}, Inputs(extract))
	s.dag.Task(consume, TaskSpec{TaskId: "load"}, Inputs(transform))

	byID := s.infoByID()
	s.Equal([]string{"transform"}, byID["extract"].Downstream)
	s.Equal([]string{"load"}, byID["transform"].Downstream)
	s.Nil(byID["load"].Downstream)

	s.Nil(byID["extract"].Inputs)
	s.Equal([]string{"extract"}, byID["transform"].Inputs)
	s.Equal([]string{"transform"}, byID["load"].Inputs)
}

func (s *RegistrySuite) TestTask_FanOutFanIn() {
	extract := s.dag.Task(produce, TaskSpec{TaskId: "extract"})
	a := s.dag.Task(relay, TaskSpec{TaskId: "transform_a"}, Inputs(extract))
	b := s.dag.Task(relay, TaskSpec{TaskId: "transform_b"}, Inputs(extract))
	s.dag.Task(consumeTwo, TaskSpec{TaskId: "load"}, Inputs(a, b))

	byID := s.infoByID()
	s.ElementsMatch([]string{"transform_a", "transform_b"}, byID["extract"].Downstream)
	s.Equal([]string{"load"}, byID["transform_a"].Downstream)
	s.Equal([]string{"load"}, byID["transform_b"].Downstream)
	s.Equal([]string{"transform_a", "transform_b"}, byID["load"].Inputs)
}

func (s *RegistrySuite) TestTask_AfterRecordsOrderingEdge() {
	first := s.dag.Task(myTask, TaskSpec{TaskId: "first"})
	s.dag.Task(myTask, TaskSpec{TaskId: "second"}, After(first))

	byID := s.infoByID()
	s.Equal([]string{"second"}, byID["first"].Downstream)
	s.Nil(byID["second"].Inputs)
}

func (s *RegistrySuite) TestTask_InputsAndAfterDeduplicateEdges() {
	extract := s.dag.Task(produce, TaskSpec{TaskId: "extract"})
	// The same upstream named as both a data input and an After ref records
	// only one downstream edge.
	s.dag.Task(consume, TaskSpec{TaskId: "load"}, Inputs(extract), After(extract))

	byID := s.infoByID()
	s.Equal([]string{"load"}, byID["extract"].Downstream)
}

func (s *RegistrySuite) TestTask_InputsCountMismatchPanics() {
	extract := s.dag.Task(produce, TaskSpec{TaskId: "extract"})
	s.PanicsWithError(
		`task "consumeTwo" in DAG "dag1" declares 2 data parameter(s) but Inputs supplies 1`,
		func() {
			s.dag.Task(consumeTwo, Inputs(extract))
		},
	)
}

func (s *RegistrySuite) TestTask_DataParamWithoutInputsPanics() {
	s.PanicsWithError(
		`task "consume" in DAG "dag1" declares 1 data parameter(s) but Inputs supplies 0`,
		func() {
			s.dag.Task(consume)
		},
	)
}

func (s *RegistrySuite) TestTask_InputTypeMismatchPanics() {
	extract := s.dag.Task(produce, TaskSpec{TaskId: "extract"})
	s.PanicsWithError(
		`task "consumeString" in DAG "dag1": input 0: task "extract" returns bundlev1.payload, which cannot fill parameter type string`,
		func() {
			s.dag.Task(consumeString, Inputs(extract))
		},
	)
}

func (s *RegistrySuite) TestTask_InputLooseMapAccepted() {
	extract := s.dag.Task(produce, TaskSpec{TaskId: "extract"})
	s.NotPanics(func() {
		s.dag.Task(consumeLoose, Inputs(extract))
	})
}

func (s *RegistrySuite) TestTask_InputFromValuelessTaskPanics() {
	first := s.dag.Task(myTask, TaskSpec{TaskId: "first"})
	s.PanicsWithError(
		`task "consume" in DAG "dag1": input task "first" returns no value; use After for an ordering-only dependency`,
		func() {
			s.dag.Task(consume, Inputs(first))
		},
	)
}

func (s *RegistrySuite) TestTask_CrossDagRefPanics() {
	other := s.reg.AddDag(DagSpec{DagId: "dag2"})
	upstream := other.Task(produce, TaskSpec{TaskId: "extract"})
	s.PanicsWithError(
		`task "consume" in DAG "dag1": Inputs ref "extract" belongs to DAG "dag2"; dependencies cannot cross dags`,
		func() {
			s.dag.Task(consume, Inputs(upstream))
		},
	)
}

func (s *RegistrySuite) TestTask_ForeignRegistryRefPanics() {
	foreign := New().AddDag(DagSpec{DagId: "dag1"})
	upstream := foreign.Task(produce, TaskSpec{TaskId: "extract"})
	s.PanicsWithError(
		`task "consume" in DAG "dag1": Inputs ref "extract" belongs to a different registry`,
		func() {
			s.dag.Task(consume, Inputs(upstream))
		},
	)
}

func (s *RegistrySuite) TestAddDag_WithSpec() {
	dag2 := s.reg.AddDag(
		DagSpec{DagId: "dag2", Schedule: "@daily", Tags: []string{"etl"}, MaxActiveRuns: 4},
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

func producePtr() (*payload, error) { return &payload{Value: 1}, nil }
func consumePtr(in *payload) error  { _ = in; return nil }

// Pointer and value shapes must be interchangeable on both sides of a data
// edge: the value crosses XCom as its JSON shape, so *T and T are equivalent.
func (s *RegistrySuite) TestTask_InputPointerValueShapesCompatible() {
	val := s.dag.Task(produce, TaskSpec{TaskId: "val"})
	ptr := s.dag.Task(producePtr, TaskSpec{TaskId: "ptr"})

	s.NotPanics(func() {
		s.dag.Task(consume, TaskSpec{TaskId: "val_to_val"}, Inputs(val))
		s.dag.Task(consumePtr, TaskSpec{TaskId: "val_to_ptr"}, Inputs(val))
		s.dag.Task(consume, TaskSpec{TaskId: "ptr_to_val"}, Inputs(ptr))
		s.dag.Task(consumePtr, TaskSpec{TaskId: "ptr_to_ptr"}, Inputs(ptr))
	})
}

func (s *RegistrySuite) TestTask_AnonymousFnWithoutIdPanics() {
	s.PanicsWithError(
		`task function in DAG "dag1" is anonymous (its derived name would be "1"); set TaskSpec.TaskId explicitly`,
		func() {
			s.dag.Task(func() error { return nil })
		},
	)
}

func (s *RegistrySuite) TestTask_AnonymousFnWithExplicitIdRegisters() {
	ref := s.dag.Task(func() error { return nil }, TaskSpec{TaskId: "inline"})
	s.Equal("inline", ref.ID())
	_, exists := s.reg.LookupTask("dag1", "inline")
	s.True(exists)
}
