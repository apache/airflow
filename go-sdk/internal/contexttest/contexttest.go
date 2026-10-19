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

// Package contexttest provides a stand-in for airflow.Context.
// It is for the tests of the packages that airflow imports.
//
// Those tests cannot import airflow back, because that would be an import cycle, so they cannot
// declare a task function that takes an airflow.Context.
// They pass [New] to binding.RegisterTaskContext and declare [Context] parameters instead.
package contexttest

import (
	"context"
	"log/slog"

	"github.com/apache/airflow/go-sdk/sdk"
)

// Context has the same methods as airflow.Context.
type Context struct {
	context.Context

	logger *slog.Logger
	client sdk.Client
	ti     sdk.TaskInstance
	dagRun sdk.DagRun
}

// New has the same signature as airflow.NewContext.
func New(
	ctx context.Context,
	logger *slog.Logger,
	client sdk.Client,
	ti sdk.TaskInstance,
	dagRun sdk.DagRun,
) Context {
	return Context{Context: ctx, logger: logger, client: client, ti: ti, dagRun: dagRun}
}

func (c Context) Logger() *slog.Logger { return c.logger }

func (c Context) Client() sdk.Client { return c.client }

func (c Context) TaskInstance() sdk.TaskInstance { return c.ti }

func (c Context) DagRun() sdk.DagRun { return c.dagRun }
