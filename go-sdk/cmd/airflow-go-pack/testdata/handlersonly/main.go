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

// Command handlersonly is a bundle fixture for the packer tests. It registers task handlers and
// no Dag from airflow.Dag.
package main

import (
	"log"

	"github.com/apache/airflow/go-sdk/airflow"
)

func extract(airflow.Context) error { return nil }

func main() {
	bundle := airflow.Bundle()
	bundle.Register(airflow.TaskHandler("py_etl", "extract", extract))
	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
