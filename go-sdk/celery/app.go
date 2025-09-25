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

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1client"
	"github.com/apache/airflow/go-sdk/pkg/bundles/shared"
)

type celeryTasksRunner struct {
	*shared.Discovery
}

func Run(ctx context.Context, config Config) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	d := shared.NewDiscovery(viper.GetString("bundles.folder"), nil)
	d.DiscoverBundles(ctx)

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

	tasks := &celeryTasksRunner{d}

	for _, queue := range config.Queues {
		app.Register(
			"execute_workload",
			queue,
			func(ctx context.Context, p *celery.TaskParam) error {
				err := tasks.ExecuteWorkloadTask(ctx, p)
				if err != nil {
					slog.ErrorContext(ctx, "Celery Task failed", "err", err)
				}
				return err
			},
		)
	}

	slog.Info("waiting for tasks", "queues", config.Queues)
	return app.Run(ctx)
}

func (state *celeryTasksRunner) ExecuteWorkloadTask(
	ctx context.Context,
	p *celery.TaskParam,
) error {
	p.NameArgs("payload")
	payload := p.MustString("payload")

	var workload bundlev1.ExecuteTaskWorkload
	if err := json.Unmarshal([]byte(payload), &workload); err != nil {
		return err
	}

	client, err := state.ClientForBundle(workload.BundleInfo.Name, workload.BundleInfo.Version)
	if err != nil {
		// TODO: This Should write something to the log file
		return err
	}
	// TODO: Don't kill the backend process here, but instead kill it after a bit of idleness. See if we can
	// reuse the process for multiple tasks too
	defer client.Kill()
	rpcClient, err := client.Client()
	if err != nil {
		return err
	}

	raw, err := rpcClient.Dispense("dag-bundle")
	if err != nil {
		return err
	}

	bundleClient := raw.(bundlev1client.BundleClient)

	return bundleClient.ExecuteTaskWorkload(ctx, workload)
}
