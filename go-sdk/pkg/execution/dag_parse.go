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
	"reflect"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// parseDags answers a DagFileParseRequest with the bundle's Dags from airflow.Dag. A Dag that
// cannot be serialized adds a line to the import error of the file, and the other Dags are still
// sent.
func parseDags(
	s bundle.DagSerializer,
	req *genmodels.DagFileParseRequest,
	logger *slog.Logger,
) genmodels.DagFileParsingResult {
	relative := computeRelativeFileloc(req.File, req.BundlePath)
	// A nil slice would be sent as null, which the Dag processor rejects.
	result := genmodels.DagFileParsingResult{
		Fileloc:        req.File,
		SerializedDags: []genmodels.LazyDeserializedDAG{},
	}
	timezone, err := parseDefaultTimezone(req.SerializationOptions.DefaultTimezone)
	if err != nil {
		result.ImportErrors = &genmodels.ImportErrors{relative: err.Error()}
		return result
	}
	var dagIDs, failures []string
	if s != nil {
		options := bundle.DagSerializationOptions{
			CreateCronDataIntervals: req.SerializationOptions.CreateCronDataIntervals,
			DefaultTimezone:         timezone,
		}
		for _, dag := range s.SerializeDags(req.File, relative, options) {
			dagIDs = append(dagIDs, dag.DagID)
			if dag.Err != nil {
				logger.Error("Dag could not be serialized", "dag_id", dag.DagID, "error", dag.Err)
				failures = append(failures, fmt.Sprintf("Dag %q: %v", dag.DagID, dag.Err))
				continue
			}
			result.SerializedDags = append(
				result.SerializedDags, genmodels.LazyDeserializedDAG{Data: dag.Data},
			)
		}
	}
	if len(failures) > 0 {
		result.ImportErrors = &genmodels.ImportErrors{relative: strings.Join(failures, "\n")}
	}
	logger.Info("Parse-mode response",
		"fileloc", req.File,
		"dag_ids", dagIDs,
		"serialized", len(result.SerializedDags),
		"import_errors", len(failures),
	)
	return result
}

// The runtime embeds time/tzdata so validation also works in bundle images without system zoneinfo.
func parseDefaultTimezone(value any) (bundle.Timezone, error) {
	if name, ok := value.(string); ok {
		if name != "" && name != "Local" {
			if _, err := time.LoadLocation(name); err == nil {
				return bundle.NamedTimezone(name), nil
			}
		}
		return bundle.Timezone{}, fmt.Errorf(
			"serialization_options.default_timezone: invalid IANA name %q",
			name,
		)
	}
	if value != nil {
		v := reflect.ValueOf(value)
		var offset int64
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			offset = v.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if v.Uint() >= 86400 {
				return bundle.Timezone{}, fmt.Errorf(
					"serialization_options.default_timezone: offset out of range",
				)
			}
			offset = int64(v.Uint())
		default:
			return bundle.Timezone{}, fmt.Errorf(
				"serialization_options.default_timezone: expected IANA name or integer offset, got %T",
				value,
			)
		}
		if offset > -86400 && offset < 86400 {
			return bundle.FixedTimezone(int(offset)), nil
		}
	}
	return bundle.Timezone{}, fmt.Errorf(
		"serialization_options.default_timezone: missing or invalid fixed offset",
	)
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
