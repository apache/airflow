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
import { useDagServiceGetDagDetails } from "openapi/queries";
import { toaster } from "src/components/ui";

export type DagParamsSpec = Record<string, ParamSpec>;

export type ParamSpec = {
  description: string | null;
  schema: ParamSchema;
  value: unknown;
};

export type ParamSchema = {
  const: string | null;
  description_md: string | null;
  enum: Array<string> | null;
  examples: Array<string> | null;
  format: string | null;
  items: Record<string, unknown> | null;
  maximum: number | null;
  maxLength: number | null;
  minimum: number | null;
  minLength: number | null;
  title: string | null;
  type: Array<string> | string | null;
  values_display: Record<string, string> | null;
};

export const useDagParams = (dagId: string, open: boolean) => {
  const { data, error }: { data?: Record<string, DagParamsSpec>; error?: unknown } =
    useDagServiceGetDagDetails({ dagId }, undefined, {
      enabled: open,
    });

  if (Boolean(error)) {
    const errorDescription =
      typeof error === "object" && error !== null
        ? JSON.stringify(error, undefined, 2) // Safely stringify the object with pretty-printing
        : String(error ?? ""); // Convert other types (e.g., numbers, strings) to string

    toaster.create({
      description: `Dag params request failed. Error: ${errorDescription}`,
      title: "Getting Dag Params Failed",
      type: "error",
    });
  }

  const transformedParams = data?.params
    ? Object.fromEntries(Object.entries(data.params).map(([key, param]) => [key, param.value]))
    : {};

  const initialConf = JSON.stringify(transformedParams, undefined, 2);

  const paramsDict: DagParamsSpec = data?.params ?? ({} as DagParamsSpec);

  return { initialConf, paramsDict };
};
