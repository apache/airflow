//# airflowBundle={"code":{"start":"0000000000000203","end":"000000000000057a","sha256":"bd26f9a6295069aef9eff45213377a49a448dcc0973da182a29ffb927f259e8e"},"metadata":{"start":"000000000000013e","end":"0000000000000202","sha256":"a51dfd6f0c9e8ea867900e55c0387b556d3cb0e98321b62d4625f522ed465041"}}
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

import { Bundle } from "../../../src/index.js";

await new Bundle().serve();
