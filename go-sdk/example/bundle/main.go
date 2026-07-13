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

package main

import (
	"fmt"
	"log"
	"log/slog"
	"runtime"
	"time"

	v1 "github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1server"
	"github.com/apache/airflow/go-sdk/example/bundle/concurrentxcom"
	"github.com/apache/airflow/go-sdk/example/bundle/taskflowbinding"
	"github.com/apache/airflow/go-sdk/sdk"
)

// Set by `-ldflags` at build time
var (
	bundleName    = "example_dags"
	bundleVersion = "0.0"
)

type myBundle struct{}

// myBundle must implement v1.BundleProvider
var _ v1.BundleProvider = (*myBundle)(nil)

func (m *myBundle) GetBundleVersion() v1.BundleInfo {
	return v1.BundleInfo{Name: bundleName, Version: &bundleVersion}
}

func (m *myBundle) RegisterDags(dagbag v1.Registry) error {
	simpleDag := dagbag.AddDag("simple_dag")
	simpleDag.AddTask(extract)
	simpleDag.AddTask(transform)
	simpleDag.AddTask(load)

	// Tasks defined in other packages register through the same dagbag.
	concurrentDag := dagbag.AddDag("concurrent_xcom_dag")
	concurrentDag.AddTaskWithName("pull_xcoms_concurrently", concurrentxcom.PullXComsConcurrently)

	bindingDag := dagbag.AddDag("taskflow_binding_dag")
	bindingDag.AddTaskWithName("make_config", taskflowbinding.MakeConfig)
	bindingDag.AddTaskWithName("make_numbers", taskflowbinding.MakeNumbers)
	bindingDag.AddTaskWithName("combine", taskflowbinding.Combine)

	return nil
}

func main() {
	if err := bundlev1server.Serve(&myBundle{}); err != nil {
		log.Fatal(err)
	}
}

func extract(ctx sdk.TIRunContext, client sdk.Client, log *slog.Logger) (any, error) {
	log.Info("Hello from task")

	// ctx behaves as a context.Context and also carries the task instance
	// identifiers and the Dag run's scheduling timestamps. Log every field the
	// runtime context exposes. The fields are namespaced under a "context"
	// group (so they serialise as context.ti.* / context.dag_run.* dotted
	// keys) to avoid colliding with the reserved task_id/run_id/etc. keys the
	// supervisor strips from its log view.
	ti, dagRun := ctx.TaskInstance(), ctx.DagRun()
	log.InfoContext(ctx, "task runtime context",
		slog.Group("context",
			slog.Group("ti",
				"dag_id", ti.DagID,
				"run_id", ti.RunID,
				"task_id", ti.TaskID,
				"map_index", ti.MapIndex,
				"try_number", ti.TryNumber,
			),
			slog.Group("dag_run",
				"dag_id", dagRun.DagID,
				"run_id", dagRun.RunID,
				"logical_date", dagRun.LogicalDate,
				"data_interval_start", dagRun.DataIntervalStart,
				"data_interval_end", dagRun.DataIntervalEnd,
			),
		),
	)

	conn, err := client.GetConnection(ctx, "test_http")
	if err != nil {
		log.ErrorContext(ctx, "unable to get conn", "error", err)
	} else {
		// Log only non-sensitive fields; conn.Password and any secrets in
		// conn.Extra must never reach the log stream.
		log.InfoContext(ctx, "got conn",
			"conn_id", conn.ID,
			"conn_type", conn.Type,
			"host", conn.Host,
			"port", conn.Port,
		)
	}
	for range 10 {

		// Once per loop,.check if we've been asked to cancel!
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		log.Info("After the beep the time will be", "time", time.Now())
		time.Sleep(2 * time.Second)
	}
	log.Info("Goodbye from task")

	ret := map[string]any{
		"go_version": runtime.Version(),
		"timestamp":  time.Now().UnixNano(),
	}

	return ret, nil
}

// transform demonstrates TaskFlow-style argument binding: the Python stub Dag
// calls “transform("uk", extract())“, so the runtime binds the "uk" literal
// onto country and pulls extract's return-value XCom into extracted -- the
// injectable parameters (runtime context, client, logger) are filled by type
// as before, in any position.
func transform(
	ctx sdk.TIRunContext,
	client sdk.VariableClient,
	log *slog.Logger,
	country string,
	extracted map[string]any,
) error {
	// This function takes a VariableClient and not a Client to make unit testing it easier. See
	// `./main_test.go` for an example unit of this task fn. Functionally taking a `sdk.Client` is the same (as
	// Client includes VariableClient) but by using the dedicated type it can be easier to write unit tests.
	//
	// It also gives a better indication of what features the tasks use
	log.InfoContext(ctx, "Bound TaskFlow arguments",
		"country", country,
		"extracted_go_version", extracted["go_version"],
		"extracted_timestamp", extracted["timestamp"],
	)
	key := "my_variable"
	val, err := client.GetVariable(ctx, key)
	if err != nil {
		return err
	}
	log.Info("Obtained variable", key, val)
	return nil
}

// load fails on its first attempt and succeeds on the retry. With retries
// configured on the stub task, the first failure makes the supervisor mark the
// task UP_FOR_RETRY -- which only works because the Go SDK now emits a
// RetryTask frame (instead of a terminal FAILED) when ti_context.should_retry
// is set. The retry then runs this task again and it returns nil.
func load(ctx sdk.TIRunContext, log *slog.Logger) error {
	tryNumber := ctx.TaskInstance().TryNumber
	if tryNumber == 1 {
		log.InfoContext(ctx, "Please fail", "try_number", tryNumber)
		return fmt.Errorf("Please fail")
	}
	log.InfoContext(ctx, "Recovered on retry", "try_number", tryNumber)
	return nil
}
