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
import type { ExternalViewResponse, ReactAppResponse, ConfigResponse } from "openapi/requests/types.gen";

// Union type for navigation items that can be either external views or react apps
export type NavItemResponse = ExternalViewResponse | ReactAppResponse;

/**
 * Plugin import error structure as returned by the config service
 */
export type PluginImportError = {
  error: string;
  source: string;
};

/**
 * Extended config response that includes plugin import errors
 */
export type ConfigWithPluginErrors = {
  plugin_import_errors?: Array<PluginImportError>;
} & ConfigResponse;

/**
 * Type guard to check if config contains plugin import errors
 */
export const isConfigWithPluginErrors = (config: unknown): config is ConfigWithPluginErrors =>
  config !== null &&
  typeof config === "object" &&
  Object.hasOwn(config, "plugin_import_errors") &&
  ((config as { plugin_import_errors?: unknown }).plugin_import_errors === undefined ||
    Array.isArray((config as { plugin_import_errors?: unknown }).plugin_import_errors));

/**
 * Safely extracts plugin import errors from config
 * @param config - The config object from the API
 * @returns Array of plugin import errors, empty array if none found
 */
export const getPluginImportErrors = (config: unknown): Array<PluginImportError> => {
  if (!isConfigWithPluginErrors(config)) {
    return [];
  }

  return config.plugin_import_errors ?? [];
};
