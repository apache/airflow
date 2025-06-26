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
import { useTranslation } from "react-i18next";

import { useDagServiceGetDagDetails } from "openapi/queries";
import { toaster } from "src/components/ui";

export type ParamsSpec = Record<string, ParamSpec>;

export type ParamSpec = {
  description: string | null;
  schema: ParamSchema;
  value: unknown;
};

export type ParamSchema = {
  // TODO define the structure on API as generated code
  const: string | undefined;
  description_md: string | undefined;
  enum: Array<string> | undefined;
  examples: Array<string> | undefined;
  format: string | undefined;
  items: Record<string, unknown> | undefined;
  maximum: number | undefined;
  maxLength: number | undefined;
  minimum: number | undefined;
  minLength: number | undefined;
  section: string | undefined;
  title: string | undefined;
  type: Array<string> | string | undefined;
  values_display: Record<string, string> | undefined;
};

export const useDagParams = (dagId: string, open: boolean) => {
  const { t: translate } = useTranslation("dag");
  const { data, error }: { data?: Record<string, ParamsSpec>; error?: unknown } = useDagServiceGetDagDetails(
    { dagId },
    undefined,
    {
      enabled: open,
    },
  );

  if (Boolean(error)) {
    const errorDescription =
      typeof error === "object" && error !== null
        ? JSON.stringify(error, undefined, 2) // Safely stringify the object with pretty-printing
        : String(Boolean(error) ? error : ""); // Convert other types (e.g., numbers, strings) to string

    toaster.create({
      description: errorDescription,
      title: translate("paramsFailed"),
      type: "error",
    });
  }

  const paramsDict: ParamsSpec = data?.params ?? ({} as ParamsSpec);

  return { paramsDict };
};
