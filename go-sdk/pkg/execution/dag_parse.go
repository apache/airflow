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
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// parseDags answers a DagFileParseRequest with the Dags from airflow.Dag that the bundle
// registered. The answer does not include the Dag of a task handler, because the Python Dag file
// that declares that Dag is parsed separately.
//
// If a Dag cannot be serialized, parseDags turns the failure into an import error that names the
// Dag. parseDags still sends the other Dags, and the Dag processor stores them. But while the
// bundle binary has an import error, the Dag processor deactivates every Dag of the bundle binary.
// The Dag processor treats a Python Dag file with an import error in the same way. The Dag
// processor stores each import error under the path of the file relative to its Dag bundle, and it
// keeps only one import error for each file. So the messages for all the Dags that cannot be
// serialized share one import error.
func parseDags(
	b bundle.DagSerializer,
	req *genmodels.DagFileParseRequest,
	logger *slog.Logger,
) genmodels.DagFileParsingResult {
	relative := computeRelativeFileloc(req.File, req.BundlePath)
	// A nil slice would be sent as null, which the Dag processor rejects.
	result := genmodels.DagFileParsingResult{
		Fileloc:        req.File,
		SerializedDags: []genmodels.LazyDeserializedDAG{},
	}
	var failures []string
	for _, dag := range b.SerializeDags(req.File, relative) {
		if dag.Err != nil {
			logger.Error("Dag could not be serialized", "dag_id", dag.DagID, "error", dag.Err)
			failures = append(failures, fmt.Sprintf("Dag %q: %v", dag.DagID, dag.Err))
			continue
		}
		result.SerializedDags = append(
			result.SerializedDags, genmodels.LazyDeserializedDAG{Data: dag.Data},
		)
	}
	if len(failures) > 0 {
		result.ImportErrors = &genmodels.ImportErrors{relative: strings.Join(failures, "\n")}
	}
	return result
}

// computeRelativeFileloc returns file relative to bundlePath, in the same way that Python's DagBag
// computes the relative_fileloc of a Dag file. Like DagBag, it returns file unchanged when file is
// not inside bundlePath.
func computeRelativeFileloc(file, bundlePath string) string {
	rel, err := filepath.Rel(bundlePath, file)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return file
	}
	return rel
}
