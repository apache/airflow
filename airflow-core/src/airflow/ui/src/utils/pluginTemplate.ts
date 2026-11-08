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

/** The entity identifiers a plugin-supplied URL can interpolate. */
export type PluginTemplateTokens = {
  assetId?: string;
  assetUri?: string;
  dagId?: string;
  mapIndex?: string;
  regionId?: string;
  regionIndex?: string;
  runId?: string;
  taskId?: string;
};

const TEMPLATE_TOKENS: Array<{ key: keyof PluginTemplateTokens; token: string }> = [
  { key: "dagId", token: "{DAG_ID}" },
  { key: "runId", token: "{RUN_ID}" },
  { key: "taskId", token: "{TASK_ID}" },
  { key: "mapIndex", token: "{MAP_INDEX}" },
  { key: "regionId", token: "{REGION_ID}" },
  { key: "regionIndex", token: "{REGION_INDEX}" },
  { key: "assetId", token: "{ASSET_ID}" },
  { key: "assetUri", token: "{ASSET_URI}" },
];

/**
 * Substitute `{DAG_ID}`-style tokens in a plugin-supplied URL.
 *
 * A token the caller has no value for is left in the string verbatim rather than replaced with
 * an empty string, so a half-resolved URL fails visibly instead of quietly pointing elsewhere.
 */
export const applyPluginTemplate = (template: string, tokens: PluginTemplateTokens): string =>
  TEMPLATE_TOKENS.reduce((result, { key, token }) => {
    const value = tokens[key];

    return value === undefined ? result : result.replaceAll(token, encodeURIComponent(value));
  }, template);
