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

package sdk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
)

func TestCurrentContext(t *testing.T) {
	logical := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	want := RuntimeContext{
		TI: TaskInstance{
			DagID:     "dag1",
			RunID:     "run1",
			TaskID:    "task1",
			MapIndex:  ptr(3),
			TryNumber: 2,
		},
		DagRun: DagRun{
			DagID:       "dag1",
			RunID:       "run1",
			LogicalDate: &logical,
		},
	}

	ctx := context.WithValue(context.Background(), sdkcontext.RuntimeContextKey, want)
	assert.Equal(t, want, CurrentContext(ctx))
}

func TestCurrentContextAbsentReturnsZero(t *testing.T) {
	assert.Equal(t, RuntimeContext{}, CurrentContext(context.Background()))
}
