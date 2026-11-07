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

/** Shape of the `usage` XCom pushed by LLMOperator/AgentOperator (see format_usage_for_xcom). */
export interface UsageInfo {
  requests: number;
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  tool_calls: number;
  cost: string | null;
}

/** Where a skill was loaded from: a local path, or a path inside a Git repository at a commit. */
export interface SkillSource {
  path: string;
  repo_url?: string;
  branch?: string | null;
  commit?: string | null;
}

/** One entry of the skills XCom pushed by AgentSkillsToolset (see toolsets/skills.py). */
export interface SkillInfo {
  name: string;
  description: string | null;
  source: SkillSource;
  /** How many times the agent called each skill tool for this skill, e.g. `{ load_skill: 1 }`. */
  calls: Record<string, number>;
}
