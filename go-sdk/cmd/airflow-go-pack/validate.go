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
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
)

const maxIDLength = 250

var idRegex = regexp.MustCompile(`^[\p{L}\p{N}_.-]+$`)

// warnOnSuspiciousIDs checks every dag and task id in the manifest against the
// rules the Airflow server enforces (airflow.utils.helpers.validate_key). It is
// best-effort and only warns: the server validates authoritatively, and checks
// like the '..' one depend on server configuration the packer cannot see.
func warnOnSuspiciousIDs(stderr io.Writer, meta airflowmetadata.Manifest) {
	dagIDs := make([]string, 0, len(meta.Dags))
	for id := range meta.Dags {
		dagIDs = append(dagIDs, id)
	}
	sort.Strings(dagIDs)
	for _, dagID := range dagIDs {
		warnOnSuspiciousID(stderr, fmt.Sprintf("dag id %q", dagID), dagID)
		for _, taskID := range meta.Dags[dagID].Tasks {
			warnOnSuspiciousID(stderr, fmt.Sprintf("task id %q in dag %q", taskID, dagID), taskID)
		}
	}
}

func warnOnSuspiciousID(stderr io.Writer, label, id string) {
	if length := utf8.RuneCountInString(id); length > maxIDLength {
		fmt.Fprintf(
			stderr,
			"warning: %s is longer than %d characters (%d); the Airflow server will reject it\n",
			label, maxIDLength, length,
		)
	}
	if !idRegex.MatchString(id) {
		fmt.Fprintf(
			stderr,
			"warning: %s must be made of alphanumeric characters, dashes, dots, and underscores; the Airflow server will reject it\n",
			label,
		)
	} else if strings.Contains(id, "..") {
		fmt.Fprintf(
			stderr,
			"warning: %s contains '..'; the Airflow server will reject it unless [core] allow_double_dot_in_ids is enabled\n",
			label,
		)
	}
}
