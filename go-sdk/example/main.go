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
	"os"
	"time"

	"github.com/MatusOllah/slogcolor"

	airflow "github.com/apache/airflow/go-sdk/celery/cmd"
	"github.com/apache/airflow/go-sdk/sdk"
	"github.com/apache/airflow/go-sdk/worker"
)

// Ensure the worker knows how to execute the tasks
func registerTasks(worker worker.Worker) {
	worker.RegisterTask("tutorial_dag", extract)
	worker.RegisterTask("tutorial_dag", transform)
	worker.RegisterTask("tutorial_dag", load)
}

func extract(ctx context.Context, log *slog.Logger) error {
	log.Info("Hello from task")
	for range 10 {

		// Once per loop,.check if we've been asked to cancel!
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		log.Info("After the beep the time will be", "time", time.Now())
		time.Sleep(2 * time.Second)
	}
	log.Info("Goodbye from task")

	return nil
}

func transform(ctx context.Context, client sdk.Client, log *slog.Logger) error {
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

func main() {
	logger := makeLogger()
	slog.SetDefault(logger)
	logger.Debug("Starting up")
	worker := worker.New(logger)
	registerTasks(worker)

	airflow.Execute(worker)
}

func makeLogger() *slog.Logger {
	opts := *slogcolor.DefaultOptions
	leveler := &slog.LevelVar{}
	leveler.Set(slog.LevelDebug)
	opts.Level = leveler
	log := slog.New(slogcolor.NewHandler(os.Stderr, &opts))
	return log
}
