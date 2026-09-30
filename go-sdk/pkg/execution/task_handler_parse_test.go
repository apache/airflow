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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/internal/contexttest"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

func TestDeclareTaskHandlers(t *testing.T) {
	registry := newHandlerRegistry()
	registry.add("etl", "transform", func(actx contexttest.Context, region string) error {
		return nil
	})
	registry.add("report", "render", simpleTask)
	registry.add("etl", "extract", simpleTask)
	// Listed, but a task run could not find it.
	registry.order = append(registry.order, bundle.TaskHandlerInfo{DagID: "etl", TaskID: "ghost"})

	result := declareTaskHandlers(registry, &genmodels.TaskHandlerParseRequest{
		File:   "/bundles/go/etl",
		DagIds: []string{"etl", "missing"},
	})

	assert.Equal(t, genmodels.TaskHandlerParsingResult{
		Fileloc: "/bundles/go/etl",
		TaskHandlers: genmodels.TaskHandlers{
			"etl": {
				{
					TaskID:  "transform",
					Binding: genmodels.TaskHandlerDeclarationBindingPositional,
					Params: []genmodels.TaskHandlerParam{
						{Required: true, ValueSchema: &genmodels.ArgValueSchema{"type": "string"}},
					},
				},
				{
					TaskID:  "extract",
					Binding: genmodels.TaskHandlerDeclarationBindingPositional,
					Params:  []genmodels.TaskHandlerParam{},
				},
			},
		},
	}, result)
}

func TestDeclareTaskHandlersMatchingNothing(t *testing.T) {
	registry := newHandlerRegistry()
	registry.add("report", "render", simpleTask)

	result := declareTaskHandlers(registry, &genmodels.TaskHandlerParseRequest{
		DagIds: []string{"etl"},
	})

	// A nil map would go out as null, which the Dag processor rejects.
	assert.Equal(t, genmodels.TaskHandlers{}, result.TaskHandlers)
}
