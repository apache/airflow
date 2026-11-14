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

// Package bundle defines what the coordinator runtime needs from a bundle: the
// tasks it looks up and runs, the Dag and task ids and the Dag source files it
// lists in the manifest, and the serialized Dags it sends to the Dag processor.
//
// Package airflow builds the tasks from both the task handlers and the Dags a
// bundle registers, the ids from its task handlers, and the source files and
// serialized Dags from its Dags.
package bundle
