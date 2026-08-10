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

// Package concurrentxcom holds the pull_xcoms_concurrently task in its own
// package, so main.go can register tasks defined across packages with one
// RegisterDags.
package concurrentxcom

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/sdk"
)

const (
	numXComs = 10
	// perItemWork is the per-item work the goroutines overlap.
	perItemWork = 150 * time.Millisecond
)

// PullXComsConcurrently pulls a batch of XComs sequentially then concurrently
// (one goroutine per item), exercising concurrent reads of the injected
// sdk.Client, and returns both timings.
func PullXComsConcurrently(ctx sdk.TIRunContext, client sdk.Client, log *slog.Logger) (any, error) {
	ti := ctx.TaskInstance()
	// PushXCom needs only the ids off the TaskInstance, not the UUID.
	apiTI := api.TaskInstance{
		DagId:    ti.DagID,
		RunId:    ti.RunID,
		TaskId:   ti.TaskID,
		MapIndex: ti.MapIndex,
	}

	keys := make([]string, numXComs)
	for i := range keys {
		keys[i] = fmt.Sprintf("item_%d", i)
		if err := client.PushXCom(ctx, apiTI, keys[i], i); err != nil {
			return nil, fmt.Errorf("seeding xcom %s: %w", keys[i], err)
		}
	}

	pull := func(key string) (any, error) {
		v, err := client.GetXCom(ctx, ti.DagID, ti.RunID, ti.TaskID, nil, key, nil)
		if err != nil {
			return nil, err
		}
		time.Sleep(perItemWork)
		return v, nil
	}

	seqResults := make([]any, numXComs)
	seqStart := time.Now()
	for i, key := range keys {
		v, err := pull(key)
		if err != nil {
			return nil, fmt.Errorf("sequential pull %s: %w", key, err)
		}
		seqResults[i] = v
	}
	sequential := time.Since(seqStart)

	concResults := make([]any, numXComs)
	errs := make([]error, numXComs)
	concStart := time.Now()
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			concResults[i], errs[i] = pull(key)
		}(i, key)
	}
	wg.Wait()
	concurrent := time.Since(concStart)
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("concurrent pulls failed: %w", err)
	}

	for i := range concResults {
		if !reflect.DeepEqual(concResults[i], seqResults[i]) {
			return nil, fmt.Errorf(
				"concurrent result %d = %v, want %v",
				i,
				concResults[i],
				seqResults[i],
			)
		}
	}

	log.InfoContext(ctx, "pulled xcoms concurrently",
		"num_xcoms", numXComs,
		"sequential_ms", sequential.Milliseconds(),
		"concurrent_ms", concurrent.Milliseconds(),
	)
	return map[string]any{
		"num_xcoms":     numXComs,
		"sequential_ms": sequential.Milliseconds(),
		"concurrent_ms": concurrent.Milliseconds(),
	}, nil
}
