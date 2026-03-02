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
	tutorial_dag := dagbag.AddDag("tutorial_dag")
	tutorial_dag.AddTask(extract)
	tutorial_dag.AddTask(transform)
	tutorial_dag.AddTask(load)

	return nil
}

func main() {
	bundlev1server.Serve(&myBundle{})
}

func extract(ctx context.Context, client sdk.Client, log *slog.Logger) (any, error) {
	log.Info("Hello from task")
	conn, err := client.GetConnection(ctx, "test_http")
	if err != nil {
		log.ErrorContext(ctx, "unable to get conn", "error", err)
	} else {
		log.InfoContext(ctx, "got conn", "conn", conn)
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
	}

	return ret, nil
}

func transform(ctx context.Context, client sdk.VariableClient, log *slog.Logger) error {
	// This function takes a VariableClient and not a Client to make unit testing it easier. See
	// `./main_test.go` for an example unit of this task fn. Functionally taking a `sdk.Client` is the same (as
	// Client includes VariableClient) but by using the dedicated type it can be easier to write unit tests.
	//
	// It also gives a better indication of what features the tasks use
	key := "my_variable"
	val, err := client.GetVariable(ctx, key)
	if err != nil {
		return err
	}
	log.Info("Obtained variable", key, val)
	return nil
}

func load() error {
	return fmt.Errorf("Please fail")
}
