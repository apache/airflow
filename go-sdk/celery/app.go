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

package celery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	celery "github.com/marselester/gopher-celery"
	celeryredis "github.com/marselester/gopher-celery/goredis"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/worker"
)

func Run(ctx context.Context, config Config) error {
	worker, ok := ctx.Value(sdkcontext.WorkerContextKey).(worker.Worker)
	if !ok {
		slog.ErrorContext(ctx, "Worker missing in context")
		return fmt.Errorf("worker value missing in context")
	}

	worker, err := worker.WithServer(viper.GetString("execution-api-url"))
	if err != nil {
		slog.ErrorContext(ctx, "Error setting ExecutionAPI sxerver for worker", "err", err)
		return err
	}

	// Store the updated worker in context
	ctx = context.WithValue(ctx, sdkcontext.WorkerContextKey, worker)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	c := redis.NewClient(&redis.Options{
		Addr: config.BrokerAddr,
	})
	defer func() {
		if err := c.Close(); err != nil {
			slog.ErrorContext(ctx, "failed to close Redis client", "err", err)
		}
	}()

	if _, err := c.Ping(ctx).Result(); err != nil {
		slog.ErrorContext(ctx, "Redis connection failed", "err", err)
		return err
	}
	broker := celeryredis.NewBroker(
		celeryredis.WithClient(c),
	)

	if len(config.Queues) == 0 {
		return fmt.Errorf("no queues defined")
	}
	broker.Observe(config.Queues)

	app := celery.NewApp(
		celery.WithBroker(broker),
	)

	for _, queue := range config.Queues {
		app.Register(
			"execute_workload",
			queue,
			func(ctx context.Context, p *celery.TaskParam) error {
				p.NameArgs("payload")
				payload := p.MustString("payload")

				var workload api.ExecuteTaskWorkload
				if err := json.Unmarshal([]byte(payload), &workload); err != nil {
					return err
				}
				return worker.ExecuteTaskWorkload(ctx, workload)
			},
		)
	}

	slog.Info("waiting for tasks", "queues", config.Queues)
	err = app.Run(ctx)
	if err != nil {
		slog.Error("program stopped", "error", err)
	}
	return err
}
