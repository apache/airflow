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

export interface ObservationNode {
  cost: number | null;
  id: string;
  /** Present (possibly null) only in trace-store mode, where IO is inline
   *  because the whole span file was read anyway; absent (undefined) in
   *  Langfuse mode, where IO is lazy-fetched per node on expand. */
  input?: unknown;
  input_tokens: number | null;
  latency: number | null;
  level: string | null;
  model: string | null;
  name: string | null;
  output?: unknown;
  output_tokens: number | null;
  parent_observation_id: string | null;
  start_time: string | null;
  status_message: string | null;
  total_tokens: number | null;
  type: string | null;
}

export interface AirflowRef {
  dag_id: string;
  map_index: number;
  run_id: string;
  task_id: string;
}

export interface ConversationMessage {
  content: string;
  role: string;
}

export interface AgentItem {
  dag_id: string;
  failed: number;
  last_run: string | null;
  operator: string | null;
  runs: number;
  task_id: string;
}

export interface ObservationIO {
  id: string;
  input: unknown;
  model_parameters: Record<string, unknown> | null;
  output: unknown;
}

export interface TraceSummary {
  airflow_ref?: AirflowRef | null;
  completion: string | null;
  conversation?: ConversationMessage[];
  cost: number | null;
  error?: string | null;
  langfuse_url: string | null;
  latency: number | null;
  metadata?: unknown;
  model: string | null;
  observation_count: number;
  observations: ObservationNode[];
  prompt: string | null;
  timestamp: string | null;
  total_tokens: number | null;
  trace_id: string;
}

export interface TraceListItem {
  cost: number | null;
  dag_id: string;
  input_preview: string | null;
  input_tokens: number | null;
  latency: number | null;
  output_tokens: number | null;
  map_index: number;
  model: string | null;
  operator: string | null;
  run_id: string;
  start_date: string | null;
  state: string | null;
  task_id: string;
  total_tokens: number | null;
  trace_id: string | null;
}
