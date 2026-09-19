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

export const FORCE_RUN_DISABLED_OPTIONS = ["upstream", "downstream"] as const;

export type ClearOptions = {
  readonly downstream: boolean;
  readonly future: boolean;
  readonly onlyFailed: boolean;
  readonly past: boolean;
  readonly upstream: boolean;
};

// A force run targets exactly the selected instance: relatives are never expanded (the API
// rejects that) and only_failed is off (a blocked instance is not in the failed state).
export const resolveClearOptions = (
  selectedOptions: ReadonlyArray<string>,
  forceRun: boolean,
): ClearOptions => ({
  downstream: !forceRun && selectedOptions.includes("downstream"),
  future: selectedOptions.includes("future"),
  onlyFailed: !forceRun && selectedOptions.includes("onlyFailed"),
  past: selectedOptions.includes("past"),
  upstream: !forceRun && selectedOptions.includes("upstream"),
});
