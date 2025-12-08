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

// Helper to extract configuration from URL search params
export const getTriggerConf = (searchParams: URLSearchParams, reservedKeys: Array<string>) => {
  const confParam = searchParams.get("conf");

  // 1. If the user provided direct JSON 'conf' param (e.g., ?conf={"foo":"bar"})
  if (confParam !== null) {
    try {
      const parsed = JSON.parse(confParam) as unknown;

      return JSON.stringify(parsed, undefined, 2);
    } catch {
      // Ignore parsing errors
    }
  }

  // 2. If the user provided individual key-value params (e.g., ?foo=bar&run_id=123)
  const collected: Record<string, unknown> = {};

  searchParams.forEach((value, key) => {
    // Do not include reserved keys (like run_id, date) in the config, as they belong to specific form fields
    if (!reservedKeys.includes(key) && key !== "conf") {
      collected[key] = value;
    }
  });

  return Object.keys(collected).length > 0 ? JSON.stringify(collected, undefined, 2) : "{}";
};
