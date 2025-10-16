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
	"errors"
)

// VariableNotFound is an error value used to signal that a variable could not be found (and that there were
// no communication issues to the API server).
//
// See the “GetVariable“ method of [VariableClient] for an example
var VariableNotFound = errors.New("variable not found")

// ConnectionNotFound is an error value used to signal that a connection could not be found (and that there were
// no communication issues to the API server).
//
// See the “GetConnection“ method of [ConnectionClient] for an example
var ConnectionNotFound = errors.New("connection not found")

var XComNotFound = errors.New("xcom not found")
