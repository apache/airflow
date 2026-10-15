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

// Package main is an example of Dags written entirely in Go with airflow.Dag, with no Python stub
// Dag. The Dag processor runs the packed binary to parse them, and the worker runs it for each
// task.
//
// Pack it with `go tool airflow-go-pack ./example/native` and put the result in a Dag bundle.
package main

import (
	"fmt"
	"log"

	"github.com/apache/airflow/go-sdk/airflow"
)

type regionRows struct {
	Region string `json:"region"`
	Rows   int    `json:"rows"`
}

type summary struct {
	Total   int `json:"total"`
	Regions int `json:"regions"`
}

// pipeline holds the tasks that the switch chooses from, because a decider returns a *TaskRef.
type pipeline struct {
	daily, weekly *airflow.TaskRef
}

func main() {
	bundle := airflow.Bundle()
	bundle.Register(
		newPipelineDag(),
		newTriggerDag(),
		newReportDag(),
	)

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}

func newPipelineDag() *airflow.DagRef {
	dag := airflow.Dag("go_native_pipeline", airflow.DagSpec{
		Queue: "golang",
		Tags:  []string{"go", "native"},
	})

	seeded := dag.Task(seed)

	extract := dag.TaskGroup("extract")
	regions := extract.TaskGroup("regions")
	north := regions.Task(
		countRows,
		airflow.TaskSpec{TaskID: "north"},
		airflow.Inputs(airflow.Literal("north"), airflow.Literal(3)),
	)
	south := regions.Task(
		countRows,
		airflow.TaskSpec{TaskID: "south"},
		airflow.Inputs(airflow.Literal("south"), airflow.Literal(2)),
	)
	summed := extract.Task(summarize, airflow.Inputs(north, south))
	seeded.Before(airflow.Label(extract, "fan out"))

	loaded := dag.Task(loadRows, airflow.Inputs(summed, airflow.Literal("s3://bucket/out")))
	empty := dag.Task(reportEmpty)
	dag.If(anyRows, airflow.Inputs(summed)).Then(loaded).Else(empty)

	p := &pipeline{
		daily:  dag.Task(publishDaily),
		weekly: dag.Task(publishWeekly),
	}
	dag.Switch(p.pick, airflow.Inputs(summed)).Case(p.daily).Case(p.weekly)

	// Each If and Switch skips one side, so cleanup has to run after a skipped upstream task.
	dag.Task(cleanup, airflow.TaskSpec{TriggerRule: airflow.TriggerRuleNoneFailedMinOneSuccess}).
		After(loaded, empty, p.daily, p.weekly)

	return dag
}

func newTriggerDag() *airflow.DagRef {
	dag := airflow.Dag(
		"go_native_trigger",
		airflow.DagSpec{Queue: "golang", Tags: []string{"go", "native"}},
	)
	dag.Task(
		airflow.TriggerDagRun(airflow.TriggerDagRunSpec{
			DagID: "go_native_report",
			Conf:  map[string]any{"triggered_by": "go_native_trigger"},
		}),
		airflow.TaskSpec{TaskID: "trigger_report"},
	)
	return dag
}

// seed reads a Variable and a Connection, and leaves a note for cleanup under its own XCom key.
func seed(actx airflow.Context) (map[string]any, error) {
	client := actx.Client()
	greeting, err := client.GetVariable(actx, "go_native_greeting")
	if err != nil {
		return nil, err
	}
	conn, err := client.GetConnection(actx, "test_http")
	if err != nil {
		return nil, err
	}
	note := fmt.Sprintf("seeded %s from %s", greeting, conn.Host)
	if err := client.PushXCom(actx, actx.TaskInstance(), "seed_note", note); err != nil {
		return nil, err
	}
	return map[string]any{"greeting": greeting, "host": conn.Host}, nil
}

func countRows(_ airflow.Context, region string, rows int) (regionRows, error) {
	return regionRows{Region: region, Rows: rows}, nil
}

func summarize(_ airflow.Context, north, south regionRows) (summary, error) {
	return summary{Total: north.Rows + south.Rows, Regions: 2}, nil
}

func anyRows(_ airflow.Context, summed summary) (bool, error) {
	return summed.Total > 0, nil
}

func loadRows(_ airflow.Context, _ summary, target string) (string, error) {
	return target, nil
}

func reportEmpty(airflow.Context) (string, error) {
	return "no rows", nil
}

// pick chooses the task that the Variable names.
func (p *pipeline) pick(actx airflow.Context, _ summary) (*airflow.TaskRef, error) {
	cadence, err := actx.Client().GetVariable(actx, "go_native_cadence")
	if err != nil {
		return nil, err
	}
	if cadence == "weekly" {
		return p.weekly, nil
	}
	return p.daily, nil
}

func publishDaily(airflow.Context) (string, error) {
	return "daily", nil
}

func publishWeekly(airflow.Context) (string, error) {
	return "weekly", nil
}

// cleanup returns the note that seed pushed.
func cleanup(actx airflow.Context) (any, error) {
	ti := actx.TaskInstance()
	return actx.Client().GetXCom(actx, ti.DagID, ti.RunID, "seed", nil, "seed_note", nil)
}
