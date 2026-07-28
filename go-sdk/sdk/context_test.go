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

	"github.com/stretchr/testify/assert"
)

type ctxKey struct{}

func TestNewTIRunContext(t *testing.T) {
	ti := TaskInstance{DagID: "dag1", RunID: "run1", TaskID: "task1", TryNumber: 2}
	dagRun := DagRun{DagID: "dag1", RunID: "run1"}

	base := context.WithValue(context.Background(), ctxKey{}, "probe-value")
	ctx := NewTIRunContext(base, ti, dagRun)

	assert.Equal(t, ti, ctx.TaskInstance())
	assert.Equal(t, dagRun, ctx.DagRun())
	assert.Equal(
		t,
		"probe-value",
		ctx.Value(ctxKey{}),
		"context behaviour must delegate to the base context",
	)
}

// A nil base context is a programming error: the runtime always binds the
// live task context, so the constructor must fail loudly rather than hand out
// a value that panics later.
func TestNewTIRunContextNilBase(t *testing.T) {
	var nilBase context.Context
	assert.Panics(t, func() {
		NewTIRunContext(nilBase, TaskInstance{}, DagRun{})
	})
}
