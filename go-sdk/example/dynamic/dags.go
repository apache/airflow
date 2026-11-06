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
	"os"

	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/airflow"
)

type dagsFile struct {
	Dags []dagEntry `yaml:"dags"`
}

type dagEntry struct {
	DagID    string `yaml:"dag_id"`
	Greeting string `yaml:"greeting"`
}

// loadDags builds one Dag for each entry of the YAML file at path.
func loadDags(path string) ([]airflow.Registerable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var file dagsFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	dags := make([]airflow.Registerable, 0, len(file.Dags))
	for _, entry := range file.Dags {
		dag := airflow.Dag(entry.DagID, airflow.DagSpec{
			Queue: "golang",
			Tags:  []string{"go", "go_dynamic"},
		})
		dag.Task(
			greet,
			airflow.TaskSpec{TaskID: "greet"},
			airflow.Inputs(airflow.Literal(entry.Greeting)),
		)
		dags = append(dags, dag)
	}
	return dags, nil
}

func greet(_ airflow.Context, greeting string) (string, error) {
	return greeting, nil
}
