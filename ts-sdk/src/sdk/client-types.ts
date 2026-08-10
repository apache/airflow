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

/** JSON-compatible value accepted by Airflow for XCom payloads. */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/**
 * Options for pulling an XCom value.
 *
 * `dagId`, `taskId`, and `runId` default to the running task's context.
 * Pass them only when pulling an XCom value from another task or run.
 */
export interface GetXComOpts {
  key: string;
  dagId?: string;
  runId?: string;
  taskId?: string;
  mapIndex?: number | null;
  includePriorDates?: boolean;
}

/**
 * Options for pushing an XCom value.
 *
 * `dagId`, `taskId`, and `runId` default to the running task's context.
 */
export interface SetXComOpts {
  key: string;
  value: JsonValue;
  dagId?: string;
  runId?: string;
  taskId?: string;
  mapIndex?: number | null;
}

/** Airflow Connection details returned by a task client. */
export interface ConnectionResult {
  id: string;
  type: string;
  host?: string | null;
  schema?: string | null;
  login?: string | null;
  password?: string | null;
  port?: number | null;
  extra?: string | null;
}
