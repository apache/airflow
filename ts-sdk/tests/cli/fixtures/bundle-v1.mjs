//# airflowBundle=eyJjb2RlIjp7InN0YXJ0IjoiMDAwMDAwMDAwMDAwMDI2MCIsImVuZCI6IjAwMDAwMDAwMDAwMDA1ZWYiLCJzaGEyNTYiOiJmODE0MzU4ZTBkNGFhNWQzOGMxMGMzNjU1MTUxNzkwNDkxNzFiYjlkNGJiYTViODBjMDZlYTU0OWQ2ZjE2MzM3In0sIm1ldGFkYXRhIjp7InN0YXJ0IjoiMDAwMDAwMDAwMDAwMDE5YiIsImVuZCI6IjAwMDAwMDAwMDAwMDAyNWYiLCJzaGEyNTYiOiJhNTFkZmQ2ZjBjOWU4ZWE4Njc5MDBlNTVjMDM4N2I1NTZkM2NiMGU5ODMyMWI2MmQ0NjI1ZjUyMmVkNDY1MDQxIn19
//# airflowMetadata={"airflow_bundle_metadata_version":"1.0","sdk":{"language":"typescript","version":"0.1.0","supervisor_schema_version":"2026-06-16"},"source":"entry.ts","dags":{"test_dag":{"tasks":["test_task"]}}}
/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { DagRegistry, serveDags } from "../../../src/index.js";

await serveDags(new DagRegistry());
