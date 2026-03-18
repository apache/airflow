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

package edge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cast"
	"github.com/spf13/viper"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1client"
	"github.com/apache/airflow/go-sdk/pkg/bundles/shared"
	"github.com/apache/airflow/go-sdk/pkg/edgeapi"
	"github.com/apache/airflow/go-sdk/pkg/logging"
	logserver "github.com/apache/airflow/go-sdk/pkg/logging/server"
)

type worker struct {
	*shared.Discovery

	hostname string
	client   edgeapi.ClientInterface
	queues   []string
	logger   *slog.Logger

	maxConcurrency int32

	freeConcurrency atomic.Int32

	// Are we currently attempting to drain jobs?
	drain bool

	activeWorkloads map[uuid.UUID]bundlev1.ExecuteTaskWorkload

	// We need to send this on most requests, so we keep a copy of it around
	sysInfo map[string]edgeapi.WorkerStateBody_Sysinfo_AdditionalProperties
}

var (
	HeartbeatInterval = 30 * time.Second
	DeregisterTimeout = 5 * time.Second
)

func Run(ctx context.Context) error {
	apiURL := viper.GetString("edge.api_url")

	hostname := viper.GetString("edge.hostname")

	if hostname == "" {
		var err error
		hostname, err = os.Hostname()
		if err != nil {
			return err
		}
	}

	w, err := NewWorker(hostname, apiURL, viper.GetString("api_auth.secret_key"),
		viper.GetStringSlice("queues"),
	)
	if err != nil {
		return err
	}

	err = w.Register(ctx)
	if err != nil {
		return err
	}

	defer w.deregister(ctx)

	return w.mainLoop(ctx)
}

func configOrDefault[T cast.Basic](key string, fallback T) T {
	x := viper.Get(key)
	if x == nil {
		return fallback
	}
	return cast.To[T](x)
}

func NewWorker(
	hostname string,
	apiURL string,
	apiJWTSecretKey string,
	queues []string,
) (*worker, error) {
	client, err := edgeapi.NewClient(apiURL, edgeapi.WithEdgeAPIJWTKey([]byte(apiJWTSecretKey)))
	if err != nil {
		return nil, err
	}

	var maxConcurrency int32 = 16

	var airflowVer, edgeVer, concurrency, freeConcurrency, goVer edgeapi.WorkerStateBody_Sysinfo_AdditionalProperties
	airflowVer.FromWorkerStateBodySysinfo0(edgeapi.WorkerStateBodySysinfo0(
		configOrDefault("edge.airflow_version", "3.1.0"),
	))
	edgeVer.FromWorkerStateBodySysinfo0(edgeapi.WorkerStateBodySysinfo0(
		configOrDefault("edge.provider_version", "1.3.1"),
	))
	concurrency.FromWorkerStateBodySysinfo1(edgeapi.WorkerStateBodySysinfo1(maxConcurrency))
	freeConcurrency.FromWorkerStateBodySysinfo1(edgeapi.WorkerStateBodySysinfo1(maxConcurrency))
	goVer.FromWorkerStateBodySysinfo0(edgeapi.WorkerStateBodySysinfo0(runtime.Version()))

	sysInfo := map[string]edgeapi.WorkerStateBody_Sysinfo_AdditionalProperties{
		"airflow_version":       airflowVer,
		"edge_provider_version": edgeVer,
		"concurrency":           concurrency,
		"free_concurrency":      freeConcurrency,
		"go_version":            goVer,
	}
	w := &worker{
		Discovery: shared.NewDiscovery(viper.GetString("bundles.folder"), nil),

		hostname:        hostname,
		queues:          queues,
		client:          client,
		sysInfo:         sysInfo,
		logger:          slog.Default().With("logger", "edge.worker"),
		maxConcurrency:  maxConcurrency,
		activeWorkloads: map[uuid.UUID]bundlev1.ExecuteTaskWorkload{},
	}

	w.logger.Info("Starting Go Edge worker", "queues", queues)

	w.freeConcurrency.Store(maxConcurrency)

	return w, nil
}

func (w *worker) Register(ctx context.Context) error {
	_, err := w.client.Worker().Register(ctx, w.hostname, &edgeapi.WorkerStateBody{
		State:   edgeapi.EdgeWorkerStateStarting,
		Sysinfo: w.sysInfo,
		Queues:  &w.queues,
	})
	return err
}

func (w *worker) deregister(ctx context.Context) {
	w.logger.Debug("Deregistering worker")
	// Create a new context that isn't cancelled to give us time to report
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DeregisterTimeout)
	defer cancel()

	_, err := w.client.Worker().SetState(ctx, w.hostname, &edgeapi.WorkerStateBody{
		State:   edgeapi.EdgeWorkerStateOffline,
		Sysinfo: w.sysInfo,
	})
	if err != nil {
		w.logger.Warn("Unable to report worker shutdown to Edge API server", "err", err)
	}
}

func (w *worker) _currentState(_ context.Context) edgeapi.EdgeWorkerState {
	free := w.freeConcurrency.Load()
	if free != w.maxConcurrency {
		if w.drain {
			return edgeapi.EdgeWorkerStateTerminating
		}
		return edgeapi.EdgeWorkerStateRunning
	} else if w.drain {
		// We were asked to drain, and we've got nothing left running
		return edgeapi.EdgeWorkerStateOffline
	}
	return edgeapi.EdgeWorkerStateIdle
}

func (w *worker) heartbeat(ctx context.Context) error {
	state := w._currentState(ctx)
	w.logger.Debug("Heartbeating", "current_state", state)
	free := w.freeConcurrency.Load()

	slot := w.sysInfo["free_concurrency"]
	slot.FromWorkerStateBodySysinfo1(edgeapi.WorkerStateBodySysinfo1(free))
	w.sysInfo["free_concurrency"] = slot

	jobsActive := len(w.activeWorkloads)

	resp, err := w.client.Worker().SetState(ctx, w.hostname, &edgeapi.WorkerStateBody{
		State:      state,
		Sysinfo:    w.sysInfo,
		Queues:     &w.queues,
		JobsActive: &jobsActive,
	})
	if err != nil {
		return err
	}

	if resp.Queues != nil {
		w.queues = *resp.Queues
	}

	switch resp.State {
	case edgeapi.EdgeWorkerStateShutdownRequest:
		w.logger.Info("Shutdown request from server!")
		w.drain = true
	}

	return nil
}

func (w *worker) mainLoop(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logServer, err := logserver.NewFromConfig(viper.GetViper())
	if err != nil {
		return err
	}

	go logServer.ListenAndServe(ctx, time.Duration(0))

	if err := w.DiscoverBundles(ctx); err != nil {
		return err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)

	// Report state to Idle
	if err := w.heartbeat(ctx); err != nil {
		return err
	}

	heartbeat := time.NewTicker(HeartbeatInterval)
	defer heartbeat.Stop()

	// Create a sub-context so we stop the fetching jobs loop when we want to shut down
	fetchJobsCtx, stopFetchJobs := context.WithCancel(ctx)
	defer stopFetchJobs()

	workloadsChan := w.fetchJobsForever(fetchJobsCtx)

	for {
		select {
		case sig := <-sigChan:
			switch sig {
			case os.Interrupt:
				if !w.drain {
					w.logger.Info(
						"Draining worker of running tasks before stopping, hit ^C again to force terminate",
					)
					w.drain = true
					w.heartbeat(ctx)
					stopFetchJobs()
				} else {
					return nil
				}
			case syscall.SIGUSR1:
				// TODO: Print stats
			}
		case <-heartbeat.C:
			err := w.heartbeat(ctx)
			if err != nil {
				// TODO: shut down the worker after too many failed heartbeats
				w.logger.Warn("Unable to send Heartbeat request", logging.AttrErr(err))
				continue
			}

			// TODO: Deal with response (drain/cordon/etc)
		case workload := <-workloadsChan:
			w.logger.Debug("Got allocation", "workload", workload)
			go w.runWorkload(ctx, workload.ConcurrencySlots, workload.ExecuteTaskWorkload)
			// ...
		case <-ctx.Done():
			w.logger.Debug("runHeartbeater stopping")
			// Something signaled us to stop.
			return nil
		}

		if w.drain && len(w.activeWorkloads) == 0 {
			return nil
		}
	}
}

type jobInfo struct {
	bundlev1.ExecuteTaskWorkload
	ConcurrencySlots int32
}

// fetchJobsForever will fetch jobs from the API server every second (if there is capacity), sending the
// resulting workloads out on the channel.
func (w *worker) fetchJobsForever(ctx context.Context) <-chan jobInfo {
	ch := make(chan jobInfo)

	go func() {
		forever := time.NewTicker(time.Second)
		defer forever.Stop()
		for {
			select {
			case <-forever.C:
				workload, slots, err := w.fetchJob(ctx)
				if err != nil {
					w.logger.Warn("Problem getting task from API server", "err", err)
				} else if workload != nil {
					ch <- jobInfo{*workload, slots}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func (w *worker) fetchJob(ctx context.Context) (*bundlev1.ExecuteTaskWorkload, int32, error) {
	free := w.freeConcurrency.Load()
	if free == 0 {
		// No free slots, no point making a request
		return nil, 0, nil
	}
	// This is super verbose, once a second, so even on debug we don't want this on
	w.logger.LogAttrs(
		ctx,
		logging.LevelTrace,
		"Asking for work",
		slog.Int("freeConcurrency", int(free)),
		slog.Int("max", int(w.maxConcurrency)),
	)
	resp, err := w.client.Jobs().Fetch(ctx, w.hostname, &edgeapi.WorkerQueuesBody{
		FreeConcurrency: int(free),
		Queues:          &w.queues,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("unable to get jobs %w", err)
	}

	if resp.TaskId == "" {
		// Empty response, got nothing!
		return nil, 0, nil
	}

	w.logger.Info("fetchJob", "resp", fmt.Sprintf("%#v\n", resp))

	// Round trip via json. Inefficient, but easy to code
	asJSON, err := json.Marshal(resp.Command)
	if err != nil {
		// TODO: Report this to API server
		return nil, 0, fmt.Errorf("unable to marshal workload %w", err)
	}

	var out bundlev1.ExecuteTaskWorkload
	err = json.Unmarshal(asJSON, &out)
	if err != nil {
		// TODO: Report this to API server
		return nil, 0, fmt.Errorf("unable to unmarshal into workload %w", err)
	}
	w.logger.Info("fetchJob", "out", fmt.Sprintf("%#v\n", out))

	return &out, int32(resp.ConcurrencySlots), nil
}

func (w *worker) runWorkload(
	ctx context.Context,
	slots int32,
	workload bundlev1.ExecuteTaskWorkload,
) error {
	var err error
	w.freeConcurrency.Add(-slots)
	w.activeWorkloads[workload.TI.Id] = workload

	mapIndex := -1
	if workload.TI.MapIndex != nil {
		mapIndex = *workload.TI.MapIndex
	}
	w.client.Jobs().State(
		ctx,
		workload.TI.DagId,
		workload.TI.TaskId,
		workload.TI.RunId,
		workload.TI.TryNumber,
		mapIndex,
		edgeapi.TaskInstanceStateRunning,
	)

	defer func() {
		jobState := edgeapi.TaskInstanceStateSuccess

		if r := recover(); r != nil {
			w.logger.Error(
				"Recovered in runWorkload",
				slog.Any("error", r),
				"stack",
				string(debug.Stack()),
			)
			jobState = edgeapi.TaskInstanceStateFailed
		} else if err != nil {
			jobState = edgeapi.TaskInstanceStateFailed
		}
		w.client.Jobs().State(
			ctx,
			workload.TI.DagId,
			workload.TI.TaskId,
			workload.TI.RunId,
			workload.TI.TryNumber,
			mapIndex,
			jobState,
		)

		w.freeConcurrency.Add(slots)
		delete(w.activeWorkloads, workload.TI.Id)
	}()

	client, err := w.ClientForBundle(workload.BundleInfo.Name, workload.BundleInfo.Version)
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
	err = bundleClient.ExecuteTaskWorkload(ctx, workload)
	return err
}
