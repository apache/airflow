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

// Package main is an example of a Dag definition that generates its Dags from a dags.yaml file
// next to the packed binary.
//
// The packer reads the Dag IDs by running the binary, so dags.yaml must sit next to the binary
// when it packs. Build the binary, put dags.yaml next to it, and pack it in place with
// --executable, which also needs --source:
//
//	go build -o ./bin/dynamic/go_dynamic_dags.raw ./example/dynamic
//	cp ./example/dynamic/dags.yaml ./bin/dynamic/dags.yaml
//	go tool airflow-go-pack --executable ./bin/dynamic/go_dynamic_dags.raw \
//	  --source ./example/dynamic/main.go --output ./bin/go_dynamic_dags
//
// Put the packed bundle and dags.yaml together in a Dag bundle. Swapping only dags.yaml there
// changes the Dags with no rebuild. Without a dags.yaml next to it, the bundle registers no Dags.
package main

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/airflow/go-sdk/airflow"
)

func main() {
	exe, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	dags, err := loadDags(filepath.Join(filepath.Dir(exe), "dags.yaml"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Fatal(err)
	}

	bundle := airflow.Bundle()
	bundle.Register(dags...)

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
