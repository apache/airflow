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

package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSchemaKeepsAFloatDefaultAFloat(t *testing.T) {
	doc, err := readSchema(filepath.FromSlash(coreSchemaPath))
	require.NoError(t, err)

	operator := doc["definitions"].(map[string]any)["operator"].(map[string]any)
	retryDelay := operator["properties"].(map[string]any)["retry_delay"].(map[string]any)
	assert.Equal(t, json.Number("300.0"), retryDelay["default"],
		"go-jsonschema reads the JSON type of a default, so 300.0 must not become 300")
}
