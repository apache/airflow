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

// Command foreignitem tries to register a type that package airflow did not define.
// It must not compile. TestRegisterableRejectsForeignTypes builds it and expects that failure.
package main

import "github.com/apache/airflow/go-sdk/airflow"

type foreignItem struct{}

// An unexported method name belongs to the package that declares it, so this method is not
// the registerable method of airflow.Registerable even though it is spelled the same.
func (foreignItem) registerable() {}

func main() {
	airflow.Bundle().Register(foreignItem{})
}
