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
	"context"
	"fmt"

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// resolveExecutionTimeout returns the task's execution_timeout in seconds from ti_context, or nil
// when it has none. The coordinator has already filled in the worker's default.
func resolveExecutionTimeout(fromContext any) (*float64, error) {
	if fromContext == nil {
		return nil, nil
	}
	seconds, ok := ifaceFloat(fromContext)
	if !ok {
		return nil, fmt.Errorf(
			"ti_context.execution_timeout has unexpected wire shape %T",
			fromContext,
		)
	}
	return &seconds, nil
}

// setExecutionTimeout tells the supervisor the task's execution_timeout and restarts its clock from
// now. The runtime does not stop the task itself: killed_task_cleanup_time after the timeout, the
// supervisor sends SIGTERM, which cancels the task's ctx.
func (c *CoordinatorClient) setExecutionTimeout(ctx context.Context, seconds float64) error {
	_, err := c.comm.Communicate(ctx, genmodels.SetExecutionTimeout{TimeoutSeconds: seconds})
	return err
}
