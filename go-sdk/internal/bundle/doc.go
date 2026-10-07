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

// Package bundle defines what the coordinator runtime needs from a bundle. The
// runtime needs the tasks to look up and run, and the Dag and task ids to list
// in the manifest. It also needs the serialized Dags to send in answer to a Dag
// parse request.
//
// Package airflow builds the tasks, the ids, and the serialized Dags from the
// task handlers and the Dags that a bundle registers.
package bundle
