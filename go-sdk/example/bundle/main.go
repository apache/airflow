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

	"github.com/apache/airflow/go-sdk/airflow"
	v1 "github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1server"
	"github.com/apache/airflow/go-sdk/example/bundle/concurrentxcom"
	"github.com/apache/airflow/go-sdk/example/bundle/taskflowbinding"
	"github.com/apache/airflow/go-sdk/example/bundle/variablewrite"
)

type myBundle struct{}

// myBundle must implement v1.BundleProvider
var _ v1.BundleProvider = (*myBundle)(nil)

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
	bindingDag.AddTaskWithName("make_region", taskflowbinding.MakeRegion)
	bindingDag.AddTaskWithName("via_flat_args", taskflowbinding.ViaFlatArgs)
	bindingDag.AddTaskWithName("via_struct_no_tags", taskflowbinding.ViaStructNoTags)
	bindingDag.AddTaskWithName("via_struct_arg_tag", taskflowbinding.ViaStructArgTag)
	bindingDag.AddTaskWithName("via_struct_unmatched_arg", taskflowbinding.ViaStructUnmatchedArg)
	bindingDag.AddTaskWithName("via_flat_map", taskflowbinding.ViaFlatMap)
	bindingDag.AddTaskWithName("via_struct_map", taskflowbinding.ViaStructMap)
	bindingDag.AddTaskWithName("via_plain_map", taskflowbinding.ViaPlainMap)

	variableWriteDag := dagbag.AddDag("variable_write_dag")
	variableWriteDag.AddTaskWithName(
		"write_and_delete_variable",
		variablewrite.WriteAndDeleteVariable,
	)

	return nil
}

func main() {
	if err := bundlev1server.Serve(&myBundle{}); err != nil {
		log.Fatal(err)
	}
}

func extract(actx airflow.Context) (any, error) {
	log := actx.Logger()
	log.Info("Hello from task")

	// actx behaves as a context.Context and also carries the task instance
	// identifiers and the Dag run's scheduling timestamps. Log every field the
	// runtime context exposes. The fields are namespaced under a "context"
	// group (so they serialise as context.ti.* / context.dag_run.* dotted
	// keys) to avoid colliding with the reserved task_id/run_id/etc. keys the
	// supervisor strips from its log view.
	ti, dagRun := actx.TaskInstance(), actx.DagRun()
	log.InfoContext(actx, "task runtime context",
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

	conn, err := actx.Client().GetConnection(actx, "test_http")
	if err != nil {
		log.ErrorContext(actx, "unable to get conn", "error", err)
	} else {
		// Log only non-sensitive fields; conn.Password and any secrets in
		// conn.Extra must never reach the log stream.
		log.InfoContext(actx, "got conn",
			"conn_id", conn.ID,
			"conn_type", conn.Type,
			"host", conn.Host,
			"port", conn.Port,
		)
	}
	for range 10 {

		// Once per loop,.check if we've been asked to cancel!
		select {
		case <-actx.Done():
			return nil, actx.Err()
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

// transform receives the stub call's literal and XCom arguments.
// See `./main_test.go` for an example unit test of this task fn.
func transform(actx airflow.Context, country string, extracted map[string]any) error {
	log := actx.Logger()
	log.InfoContext(actx, "Bound TaskFlow arguments",
		"country", country,
		"extracted_go_version", extracted["go_version"],
		"extracted_timestamp", extracted["timestamp"],
	)
	key := "my_variable"
	val, err := actx.Client().GetVariable(actx, key)
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
func load(actx airflow.Context) error {
	tryNumber := actx.TaskInstance().TryNumber
	if tryNumber == 1 {
		actx.Logger().InfoContext(actx, "Please fail", "try_number", tryNumber)
		return fmt.Errorf("Please fail")
	}
	actx.Logger().InfoContext(actx, "Recovered on retry", "try_number", tryNumber)
	return nil
}
