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
	"context"
	"fmt"
	"log"
	"log/slog"
	"runtime"
	"time"

	v1 "github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1server"
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

	return nil
}

func main() {
	if err := bundlev1server.Serve(&myBundle{}); err != nil {
		log.Fatal(err)
	}
}

// ExtractResult is extract's return value. Returning it from the task pushes it
// as the task's return_value XCom, ready for a downstream task to pull.
type ExtractResult struct {
	GoVersion string `json:"go_version"`
	Timestamp int64  `json:"timestamp"`
}

// ExtractInput declares extract's XCom inputs. The `xcom:"python_task_1"` tag
// binds this field to the return_value of the upstream Python task
// `python_task_1`, so the Go task receives a Python task's output without
// calling client.GetXCom itself. The runtime pulls and decodes it before
// extract runs.
type ExtractInput struct {
	FromPython string `xcom:"python_task_1"`
}

// TransformInput binds transform's only XCom input to extract's return_value.
// Because the field is a dedicated struct, decoding is strict: a renamed or
// unexpected key fails the task instead of silently leaving fields zero. To
// decode loosely (e.g. for an evolving or cross-language producer), type the
// field map[string]any instead.
type TransformInput struct {
	Extracted ExtractResult `xcom:"extract"`
}

// TransformResult is transform's return value.
type TransformResult struct {
	Variable  string        `json:"variable"`
	Extracted ExtractResult `json:"extracted"`
}

// LoadInput binds load's parameter to transform's return_value.
type LoadInput struct {
	Transformed TransformResult `xcom:"transform"`
}

func extract(
	ctx context.Context,
	client sdk.Client,
	log *slog.Logger,
	in ExtractInput,
) (ExtractResult, error) {
	log.Info("Hello from task")

	// Log every field the runtime context exposes. The fields are namespaced
	// under a "context" group (so they serialise as context.ti.* /
	// context.dag_run.* dotted keys) to avoid colliding with the reserved
	// task_id/run_id/etc. keys the supervisor strips from its log view.
	rc := sdk.CurrentContext(ctx)
	log.InfoContext(ctx, "task runtime context",
		slog.Group("context",
			slog.Group("ti",
				"dag_id", rc.TI.DagID,
				"run_id", rc.TI.RunID,
				"task_id", rc.TI.TaskID,
				"map_index", rc.TI.MapIndex,
				"try_number", rc.TI.TryNumber,
			),
			slog.Group("dag_run",
				"dag_id", rc.DagRun.DagID,
				"run_id", rc.DagRun.RunID,
				"logical_date", rc.DagRun.LogicalDate,
				"data_interval_start", rc.DagRun.DataIntervalStart,
				"data_interval_end", rc.DagRun.DataIntervalEnd,
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
			return ExtractResult{}, ctx.Err()
		default:
		}
		log.Info("After the beep the time will be", "time", time.Now())
		time.Sleep(2 * time.Second)
	}
	log.Info("Goodbye from task")

	return ExtractResult{
		GoVersion: runtime.Version(),
		Timestamp: time.Now().UnixNano(),
	}, nil
}

func transform(
	ctx context.Context,
	client sdk.VariableClient,
	log *slog.Logger,
	in TransformInput,
) (TransformResult, error) {
	// `in.Extracted` is the Taskflow-injected return value of the upstream
	// `extract` task. The explicit client-pull pattern still works alongside
	// it: here we also read a Variable directly. transform takes a
	// VariableClient (not the full sdk.Client) to make unit testing easier --
	// see `./main_test.go`.
	// Note: avoid an attribute literally named "timestamp" — it is a reserved
	// field in the structured log records the coordinator sends to the
	// supervisor (which parses it as an RFC3339 datetime).
	log.Info("Got upstream XCom from 'extract'",
		"go_version", in.Extracted.GoVersion,
		"extracted_timestamp", in.Extracted.Timestamp,
	)

	key := "my_variable"
	val, err := client.GetVariable(ctx, key)
	if err != nil {
		return TransformResult{}, err
	}
	log.Info("Obtained variable", key, val)

	return TransformResult{Variable: val, Extracted: in.Extracted}, nil
}

func load(log *slog.Logger, in LoadInput) error {
	log.Info(
		"Got upstream XCom from 'transform'",
		"variable",
		in.Transformed.Variable,
		"extracted",
		in.Transformed.Extracted,
	)
	return fmt.Errorf("Please fail")
}
