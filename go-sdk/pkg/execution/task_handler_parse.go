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

package execution

import (
	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// declareTaskHandlers answers a TaskHandlerParseRequest with the declarations of every
// registered task handler, keyed by Dag id, each Dag's in registration order.
func declareTaskHandlers(
	b bundle.Registry,
	req *genmodels.TaskHandlerParseRequest,
) genmodels.TaskHandlerParsingResult {
	// A nil map would go out as null, which the Dag processor rejects.
	handlers := genmodels.TaskHandlers{}
	for _, info := range b.ListTaskHandlers() {
		// Declare only what a task run could find.
		task, ok := b.LookupTask(info.DagID, info.TaskID)
		if !ok {
			continue
		}
		handlers[info.DagID] = append(handlers[info.DagID], task.Declare(info.TaskID))
	}
	return genmodels.TaskHandlerParsingResult{Fileloc: req.File, TaskHandlers: handlers}
}
