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

/** Terminal session states (no further human action expected). */
export function isTerminalStatus(s: Pick<SessionResponse, "status" | "task_completed">): boolean {
  return (
    s.status === "approved" ||
    s.status === "rejected" ||
    s.status === "max_iterations_exceeded" ||
    s.status === "timeout_exceeded" ||
    s.task_completed
  );
}

export type SessionStatus =
  | "pending_review"
  | "changes_requested"
  | "approved"
  | "rejected"
  | "max_iterations_exceeded"
  | "timeout_exceeded";

export interface ConversationEntry {
  role: "assistant" | "human";
  content: string;
  iteration: number;
  timestamp?: string;
}

export interface SessionResponse {
  dag_id: string;
  run_id: string;
  task_id: string;
  status: SessionStatus;
  iteration: number;
  max_iterations: number;
  prompt: string;
  current_output: string;
  conversation: ConversationEntry[];
  task_completed: boolean;
}
