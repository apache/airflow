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

package airflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	flag "github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/pkg/execution"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

func TestDecideMode(t *testing.T) {
	tests := []struct {
		name     string
		metadata bool
		comm     string
		logs     string
		want     serveMode
	}{
		{name: "metadata", metadata: true, want: modeAirflowMetadata},
		{name: "coordinator", comm: "127.0.0.1:1", logs: "127.0.0.1:2", want: modeCoordinator},
		{name: "no flags", want: modeCoordinatorUsageError},
		{name: "comm only", comm: "127.0.0.1:1", want: modeCoordinatorUsageError},
		{name: "logs only", logs: "127.0.0.1:2", want: modeCoordinatorUsageError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, decideMode(tt.metadata, tt.comm, tt.logs))
		})
	}
}

func etlBundle() *BundleRef {
	b := Bundle()
	b.Register(
		TaskHandler("py_etl", "transform", noop),
		TaskHandler("py_etl", "extract", noop),
	)
	return b
}

type manifest struct {
	SDK struct {
		Language string `json:"language" yaml:"language"`
	} `json:"sdk"  yaml:"sdk"`
	Dags map[string]struct {
		Tasks []string `json:"tasks" yaml:"tasks"`
	} `json:"dags" yaml:"dags"`
}

func TestServePrintsAirflowMetadata(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		unmarshal func([]byte, any) error
	}{
		{name: "yaml by default", args: []string{"--airflow-metadata"}, unmarshal: yaml.Unmarshal},
		{
			name:      "json",
			args:      []string{"--airflow-metadata", "--format", "json"},
			unmarshal: json.Unmarshal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			require.NoError(t, etlBundle().serve(tt.args, &stdout))

			var got manifest
			require.NoError(t, tt.unmarshal(stdout.Bytes(), &got))
			assert.Equal(t, "go", got.SDK.Language)
			require.Contains(t, got.Dags, "py_etl")
			assert.Equal(t, []string{"transform", "extract"}, got.Dags["py_etl"].Tasks)
		})
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestServeReportsManifestWriteError(t *testing.T) {
	wantErr := errors.New("stdout is closed")
	err := etlBundle().serve([]string{"--airflow-metadata"}, failingWriter{wantErr})
	require.ErrorIs(t, err, wantErr)
}

func TestServeAcceptsFlagsTheBundleDefines(t *testing.T) {
	saved := flag.CommandLine
	t.Cleanup(func() { flag.CommandLine = saved })
	flag.CommandLine = flag.NewFlagSet("bundle", flag.ContinueOnError)
	region := flag.String("region", "", "a flag the bundle author defined")

	var stdout bytes.Buffer
	args := []string{"--region", "us", "--airflow-metadata"}
	require.NoError(t, etlBundle().serve(args, &stdout))

	assert.Equal(t, "us", *region)
	assert.Contains(t, stdout.String(), "py_etl")
}

func TestServeRejectsBundleFlagWithReservedName(t *testing.T) {
	for _, name := range []string{"airflow-metadata", "format", "comm", "logs"} {
		t.Run(name, func(t *testing.T) {
			saved := flag.CommandLine
			t.Cleanup(func() { flag.CommandLine = saved })
			flag.CommandLine = flag.NewFlagSet("bundle", flag.ContinueOnError)
			flag.String(name, "", "a flag the bundle author defined")

			var stdout bytes.Buffer
			err := etlBundle().serve([]string{"--airflow-metadata"}, &stdout)

			require.EqualError(t, err,
				"the bundle defines a --"+name+" flag, but Serve reserves that name")
			assert.Empty(t, stdout.String())
		})
	}
}

// pflag prints a deprecation warning while it parses, so it must not reach the writer that
// carries the --airflow-metadata manifest.
func TestServeKeepsDeprecationWarningOutOfTheManifest(t *testing.T) {
	saved := flag.CommandLine
	t.Cleanup(func() { flag.CommandLine = saved })
	flag.CommandLine = flag.NewFlagSet("bundle", flag.ContinueOnError)
	flag.String("region", "", "a flag the bundle author defined")
	require.NoError(t, flag.CommandLine.MarkDeprecated("region", "use --zone"))

	var stdout bytes.Buffer
	args := []string{"--region", "us", "--airflow-metadata", "--format", "json"}
	require.NoError(t, etlBundle().serve(args, &stdout))

	assert.NotContains(t, stdout.String(), "deprecated")
	var got manifest
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &got))
}

func TestServeRejectsBadFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantIs  error
		wantMsg string
	}{
		{name: "no flags", wantIs: errCoordinatorFlagsRequired},
		{
			name:   "comm only",
			args:   []string{"--comm", "127.0.0.1:1"},
			wantIs: errCoordinatorFlagsRequired,
		},
		{
			name:   "format without metadata",
			args:   []string{"--comm", "127.0.0.1:1", "--logs", "127.0.0.1:2", "--format", "json"},
			wantIs: errFormatRequiresMetadata,
		},
		{
			name:   "default format spelled out without metadata",
			args:   []string{"--comm", "127.0.0.1:1", "--logs", "127.0.0.1:2", "--format", "yaml"},
			wantIs: errFormatRequiresMetadata,
		},
		{
			name:    "unknown metadata format",
			args:    []string{"--airflow-metadata", "--format", "xml"},
			wantMsg: `unsupported --airflow-metadata format "xml"`,
		},
		{
			name:    "unknown flag",
			args:    []string{"--no-such-flag"},
			wantMsg: "unknown flag: --no-such-flag",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := etlBundle().serve(tt.args, &stdout)

			require.Error(t, err)
			if tt.wantIs != nil {
				require.ErrorIs(t, err, tt.wantIs)
			} else {
				assert.Contains(t, err.Error(), tt.wantMsg)
			}
			assert.Empty(t, stdout.String())
		})
	}
}

func TestServeHelpIsNotAnError(t *testing.T) {
	var stdout bytes.Buffer
	require.NoError(t, etlBundle().serve([]string{"--help"}, &stdout))

	assert.Contains(t, stdout.String(), "--airflow-metadata")
}

// serveForSupervisor runs b.serve with --comm and --logs and returns the supervisor's end of the
// comm socket and the channel that gets what serve returns.
func serveForSupervisor(t *testing.T, b *BundleRef) (*execution.CoordinatorComm, <-chan error) {
	t.Helper()
	commLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { commLn.Close() })
	logsLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { logsLn.Close() })
	// Without a deadline, a Serve that never dials would leave Accept blocked until the test
	// binary times out.
	deadline := time.Now().Add(10 * time.Second)
	require.NoError(t, commLn.(*net.TCPListener).SetDeadline(deadline))
	require.NoError(t, logsLn.(*net.TCPListener).SetDeadline(deadline))

	done := make(chan error, 1)
	go func() {
		done <- b.serve(
			[]string{"--comm", commLn.Addr().String(), "--logs", logsLn.Addr().String()},
			io.Discard,
		)
	}()

	commConn, err := commLn.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { commConn.Close() })
	logsConn, err := logsLn.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { logsConn.Close() })
	require.NoError(t, commConn.SetDeadline(deadline))

	return execution.NewCoordinatorComm(commConn, commConn, discardLogger()), done
}

// A fake supervisor sends StartupDetails over the comm socket, as the Python
// ExecutableCoordinator does after it starts the bundle with --comm and --logs.
func TestServeRunsTaskForSupervisor(t *testing.T) {
	ran := false
	b := Bundle()
	b.Register(TaskHandler("py_etl", "transform", func(Context) error {
		ran = true
		return nil
	}))

	supervisor, done := serveForSupervisor(t, b)
	require.NoError(t, supervisor.SendRequest(0, map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":         "550e8400-e29b-41d4-a716-446655440000",
			"dag_id":     "py_etl",
			"task_id":    "transform",
			"run_id":     "run1",
			"try_number": 1,
		},
		"bundle_info": map[string]any{"name": "test", "version": "1.0"},
	}))

	frame, err := supervisor.ReadMessage()
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, msgpack.Unmarshal(frame.Body, &body))
	assert.Equal(t, "SucceedTask", body["type"])

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after the task finished")
	}
	assert.True(t, ran)
}

// The Dag processor sends a DagFileParseRequest instead of StartupDetails.
func TestServeAnswersTheDagParseRequestWithoutRunningATask(t *testing.T) {
	var ran []string
	record := func(name string) func(Context) error {
		return func(Context) error {
			ran = append(ran, name)
			return nil
		}
	}
	etl := Dag("etl")
	extracted := etl.Task(func(Context) (int, error) {
		ran = append(ran, "extract")
		return 3, nil
	}, TaskSpec{TaskID: "extract"})
	loaded := etl.Task(record("load"), TaskSpec{TaskID: "load"})
	etl.If(func(_ Context, rows int) (bool, error) {
		ran = append(ran, "has_rows")
		return rows > 0, nil
	}, Inputs(extracted), TaskSpec{TaskID: "has_rows"}).Then(loaded)
	reports := Dag("reports")
	reports.Task(record("publish"), TaskSpec{TaskID: "publish"})

	b := Bundle()
	b.Register(etl, TaskHandler("py_etl", "transform", record("transform")), reports)

	supervisor, done := serveForSupervisor(t, b)
	const requestID = 7
	require.NoError(t, supervisor.SendRequest(requestID, map[string]any{
		"type":        "DagFileParseRequest",
		"file":        "/bundles/go/etl",
		"bundle_path": "/bundles/go",
		"bundle_name": "go",
	}))

	frame, err := supervisor.ReadMessage()
	require.NoError(t, err)
	assert.EqualValues(t, requestID, frame.ID)
	var result genmodels.DagFileParsingResult
	require.NoError(t, msgpack.Unmarshal(frame.Body, &result))
	assert.Equal(t, "DagFileParsingResult", result.Type)
	assert.Equal(t, "/bundles/go/etl", result.Fileloc)
	assert.Nil(t, result.ImportErrors)
	require.Len(t, result.SerializedDags, 2)
	for i, dag := range []*DagRef{etl, reports} {
		want := jsonOf(t, dag.serialize("/bundles/go/etl", "etl"))
		assertJSON(t, want, result.SerializedDags[i].Data)
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after it answered the Dag parse request")
	}
	assert.Empty(t, ran)
}

func jsonOf(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return string(raw)
}
