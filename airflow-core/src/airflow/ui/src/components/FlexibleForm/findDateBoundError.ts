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
import dayjs from "dayjs";

import type { ParamsSpec } from "src/queries/useDagParams";

// The ajv-formats keywords that bound a date param by another param, given as {"$data": "1/<param>"}.
const dateBounds = {
  formatExclusiveMinimum: {
    holds: (diff: number) => diff > 0,
    messageKey: "flexibleForm.validationErrorAfter",
  },
  formatMinimum: { holds: (diff: number) => diff >= 0, messageKey: "flexibleForm.validationErrorOnOrAfter" },
} as const;

export type DateBoundError = {
  bound: string;
  messageKey: (typeof dateBounds)[keyof typeof dateBounds]["messageKey"];
};

const parseDate = (value: unknown) => (typeof value === "string" && value !== "" ? dayjs(value) : undefined);

/** Find the date bound a param breaks, mirroring the check when a Dag run is created. */
export const findDateBoundError = (name: string, paramsDict: ParamsSpec): DateBoundError | undefined => {
  const param = paramsDict[name];

  if (param?.schema.format !== "date" && param?.schema.format !== "date-time") {
    return undefined;
  }

  const value = parseDate(param.value);

  for (const [keyword, { holds, messageKey }] of Object.entries(dateBounds)) {
    const pointer = param.schema[keyword as keyof typeof dateBounds]?.$data;
    const other = pointer?.startsWith("1/") ? pointer.slice(2) : undefined;
    const otherParam = other === undefined ? undefined : paramsDict[other];
    const bound = parseDate(otherParam?.value);

    if (value?.isValid() && bound?.isValid() && !holds(value.diff(bound))) {
      return { bound: otherParam?.schema.title ?? other ?? "", messageKey };
    }
  }

  return undefined;
};
