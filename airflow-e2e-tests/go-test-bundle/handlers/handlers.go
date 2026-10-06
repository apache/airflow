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

// Package handlers holds the task handlers of the Go test bundle of the e2e tests.
//
// The bundle is separate from the user-facing example in go-sdk, so that fixtures the e2e tests
// use to check failures cannot change what the example registers.
package handlers

import (
	"github.com/apache/airflow/go-sdk/airflow"
)

// TakesTwoNumbers takes flat parameters, so a Dag file calls it with positional arguments. A call
// with any other number of arguments does not match.
func TakesTwoNumbers(actx airflow.Context, first int, second int) error {
	actx.Logger().InfoContext(actx, "Took two numbers", "first", first, "second", second)
	return nil
}
