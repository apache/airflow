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
import type { ParamsSpec, ParamSpec } from "src/queries/useDagParams";

// Python str() of a string, integer, boolean or null parent value, because the serialized Dag
// stores the keys of options_map with str()
const getOptionKey = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "None";
  }
  if (typeof value === "boolean") {
    return value ? "True" : "False";
  }

  return typeof value === "number" || typeof value === "string" ? String(value) : JSON.stringify(value);
};

export const getParamOptions = (param: ParamSpec, paramsDict: ParamsSpec, defaults: ParamsSpec) => {
  const { depends_on: dependsOn, options_map: optionsMap } = param.schema;

  if (dependsOn === undefined) {
    return param.schema.enum;
  }

  // A JSON config can omit the parent, and the backend then validates against its default.
  // A parent that is present but null is not replaced, so a cleared parent clears its children.
  const parentValue = (paramsDict[dependsOn] ?? defaults[dependsOn])?.value;

  return optionsMap?.[getOptionKey(parentValue)] ?? [];
};
