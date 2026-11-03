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

package binding

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/apache/airflow/go-sdk/sdk"
)

// RegisterTaskContext sets all three.
var (
	airflowContextType    reflect.Type
	airflowContextPtrType reflect.Type
	newAirflowContext     func(
		context.Context, *slog.Logger, sdk.Client, sdk.TaskInstance, sdk.DagRun,
	) reflect.Value
)

// RegisterTaskContext tells this package that a task function parameter of type T receives
// the task's context, and that build creates its value.
//
// Package airflow registers airflow.Context from an init function.
// This package cannot import airflow to name that type, because airflow imports
// the packages that run a bundle, and those packages import this one.
//
// RegisterTaskContext panics if a type is already registered. A second registration would
// replace the first for the whole process, and task functions that take the first type would
// stop being accepted.
func RegisterTaskContext[T any](
	build func(context.Context, *slog.Logger, sdk.Client, sdk.TaskInstance, sdk.DagRun) T,
) {
	if airflowContextType != nil {
		panic(fmt.Sprintf(
			"binding.RegisterTaskContext: %s is already registered, cannot also register %s",
			airflowContextType, reflect.TypeFor[T](),
		))
	}
	airflowContextType = reflect.TypeFor[T]()
	airflowContextPtrType = reflect.TypeFor[*T]()
	newAirflowContext = func(
		ctx context.Context,
		logger *slog.Logger,
		client sdk.Client,
		ti sdk.TaskInstance,
		dagRun sdk.DagRun,
	) reflect.Value {
		return reflect.ValueOf(build(ctx, logger, client, ti, dagRun))
	}
}
