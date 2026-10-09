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

// Command multidag is a bundle fixture for the packer tests. It declares Dags in its own file,
// in an imported package, and through a factory in a third package.
package main

import (
	"log"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/cmd/airflow-go-pack/testdata/multidag/factory"
	"github.com/apache/airflow/go-sdk/cmd/airflow-go-pack/testdata/multidag/reports"
)

func extract(airflow.Context) error { return nil }

func main() {
	bundle := airflow.Bundle()

	orders := airflow.Dag("orders")
	orders.Task(extract)

	bundle.Register(
		airflow.TaskHandler("py_etl", "extract", extract),
		orders,
		reports.Dag(),
		factory.New("billing"),
		factory.New("shipping"),
	)

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
