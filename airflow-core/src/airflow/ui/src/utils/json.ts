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

/**
 * Checks if a string is valid JSON
 * @param jsonString - The string to validate
 * @returns true if the string is valid JSON, false otherwise
 */
export const isJsonString = (jsonString: string): boolean => {
  try {
    JSON.parse(jsonString);
  } catch {
    return false;
  }

  return true;
};

/**
 * Prettifies a JSON string with indentation
 * @param jsonString - The JSON string to prettify
 * @param indent - Number of spaces for indentation (default: 2)
 * @returns Prettified JSON string, or the original string if parsing fails
 */
export const prettifyJson = (jsonString: string, indent: number = 2): string => {
  try {
    const parsed: unknown = JSON.parse(jsonString);

    return JSON.stringify(parsed, undefined, indent);
  } catch {
    return jsonString;
  }
};

/**
 * Minifies a JSON string by removing whitespace
 * @param jsonString - The JSON string to minify
 * @returns Minified JSON string, or the original string if parsing fails
 */
export const minifyJson = (jsonString: string): string => {
  try {
    const parsed: unknown = JSON.parse(jsonString);

    return JSON.stringify(parsed);
  } catch {
    return jsonString;
  }
};
