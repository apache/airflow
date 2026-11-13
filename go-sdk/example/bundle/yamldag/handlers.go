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

// Package yamldag holds the handler behind a "run:" task of a Dag written in
// YAML. The Dag declares the task, its arguments and the queue; this package
// supplies the code, exactly as it would for a @task.stub written in Python.
package yamldag

import (
	"fmt"

	"github.com/apache/airflow/go-sdk/airflow"
)

// Summarize is the body of the "summarize" task of the yaml_s3_roundtrip Dag.
//
// Both arguments come from the Dag's "run:" mapping: label is a literal, and
// keys is the return value of the upstream S3 listing, bound through {$x: ...}.
func Summarize(actx airflow.Context, keys []string, label string) (any, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("the upstream listing bound no keys (label %q)", label)
	}
	actx.Logger().InfoContext(actx, "Summarizing the YAML Dag's listing",
		"label", label,
		"count", len(keys),
		"first_key", keys[0],
	)
	return map[string]any{"label": label, "count": len(keys), "first_key": keys[0]}, nil
}
