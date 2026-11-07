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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/airflow/go-sdk/internal/contexttest"
	"github.com/apache/airflow/go-sdk/sdk"
)

func init() { RegisterTaskContext(contexttest.New) }

func TestRegisterTaskContextRejectsSecondRegistration(t *testing.T) {
	type otherContext struct{ context.Context }

	// If RegisterTaskContext did not panic, this call would replace the registered type for every
	// later test, so the test puts the old registration back.
	savedType, savedPtrType := airflowContextType, airflowContextPtrType
	savedBuild := newAirflowContext
	t.Cleanup(func() {
		airflowContextType, airflowContextPtrType = savedType, savedPtrType
		newAirflowContext = savedBuild
	})

	assert.PanicsWithValue(t,
		"binding.RegisterTaskContext: contexttest.Context is already registered, "+
			"cannot also register binding.otherContext",
		func() {
			RegisterTaskContext(func(
				ctx context.Context, _ *slog.Logger, _ sdk.Client, _ sdk.TaskInstance, _ sdk.DagRun,
			) otherContext {
				return otherContext{ctx}
			})
		},
	)
}
