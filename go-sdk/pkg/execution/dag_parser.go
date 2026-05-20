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
	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
)

// ParseDags processes a DagFileParseRequest by serialising every dag
// registered on bundle to DagSerialization v3 and returning the result as a
// DagFileParsingResult body. bundle is the materialised registry produced by
// running BundleProvider.RegisterDags.
func ParseDags(bundle bundlev1.Bundle, req *DagFileParseRequest) map[string]any {
	fileloc := req.File
	bundlePath := req.BundlePath
	relativeFileloc := computeRelativeFileloc(fileloc, bundlePath)

	var dags []bundlev1.DagInfo
	if lister, ok := bundle.(bundlev1.DagLister); ok {
		dags = lister.ListDags()
	}

	serializedDags := make([]any, 0, len(dags))
	for _, d := range dags {
		serializedDags = append(serializedDags, map[string]any{
			"data": map[string]any{
				"__version": 3,
				"dag":       SerializeDag(d, fileloc, relativeFileloc),
			},
		})
	}

	return map[string]any{
		"type":            "DagFileParsingResult",
		"fileloc":         fileloc,
		"serialized_dags": serializedDags,
	}
}
