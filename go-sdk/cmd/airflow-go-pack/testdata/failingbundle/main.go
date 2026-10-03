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

// Command failingbundle is a bundle binary that links go-sdk and exits non-zero when it runs.
// The packer's tests pack it to show that packing never runs the bundle binary.
package main

import (
	"fmt"
	"os"

	"github.com/apache/airflow/go-sdk/airflow"
)

func main() {
	_ = airflow.Bundle()
	fmt.Fprintln(os.Stderr, "failingbundle: this binary must not run")
	os.Exit(3)
}
