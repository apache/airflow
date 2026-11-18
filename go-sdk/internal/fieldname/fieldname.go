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

// Package fieldname folds the name of a Go field and the name it is matched
// against onto one token. The Dag run conf reader in sdk and the TaskFlow
// argument binder in pkg/binding both match names this way. pkg/binding
// imports sdk, and an exported helper in sdk would become public API, so the
// shared helper lives here.
package fieldname

import "strings"

// Fold lowercases name and drops underscores, so target_table matches TargetTable.
func Fold(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "_", ""))
}
