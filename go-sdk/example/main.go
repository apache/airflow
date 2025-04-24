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
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/MatusOllah/slogcolor"
	"github.com/apache/airflow/go-sdk/sdk"
	"github.com/apache/airflow/go-sdk/worker"
)

// Ensure the worker knows how to execute the tasks
func registerTasks(worker worker.Worker) {
	worker.RegisterTask("tutorial_dag", extract)
	worker.RegisterTask("tutorial_dag", transform)
	worker.RegisterTask("tutorial_dag", load)
}

func extract(log *slog.Logger) error {
	log.Info("Hello from task")
	for range 10 {
		log.Info("After the beep the time will be", "time", time.Now())
		time.Sleep(2 * time.Second)
	}
	log.Info("Goodbye from task")

	return nil
}

func transform(ctx context.Context, log *slog.Logger) error {
	key := "my_variable"
	val, err := sdk.VariableGet(ctx, key)
	if err != nil {
		return err
	}
	log.Info("Obtained variable", key, val)
	return nil
}

func load() error {
	return fmt.Errorf("Please fail")
}

func main() {
	// defer logger.Sync() // flushes buffer, if any

	worker := worker.New(logger)
	registerTasks(worker)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	worker.RunForever(ctx, getServerUrl())
}

func makeLogger() *slog.Logger {
	log := slog.New(slogcolor.NewHandler(os.Stderr, slogcolor.DefaultOptions))
	slog.SetDefault(log)
	return log
}

func getServerUrl() string {
	// Very simple for now
	url, err := url.Parse(os.Args[1])
	if err != nil {
		logger.Error("Invalid server url", "error", err)
		os.Exit(1)
	}
	return url.String()
}

var logger = makeLogger()
