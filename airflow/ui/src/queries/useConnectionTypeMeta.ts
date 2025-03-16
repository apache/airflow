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
import { useConnectionServiceHookMetaData } from "openapi/queries";
import { toaster } from "src/components/ui";

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

type ParamsSpec = Record<string, ParamSpec>;

type StandardFieldSpec = Record<string, StandardFieldSchema>;

type StandardFieldSchema = {
  hidden?: boolean | undefined;
  placeholder?: string | undefined;
  title?: string | undefined;
};

type ConnectionMetaEntry = {
  connection_type: string;
  default_conn_name: string | undefined;
  extra_fields: ParamsSpec;
  hook_class_name: string;
  hook_name: string;
  standard_fields: StandardFieldSpec | undefined;
};

type ConnectionMeta = Array<ConnectionMetaEntry>;

export const useConnectionTypeMeta = () => {
  const { data, error }: { data?: ConnectionMeta; error?: unknown } = useConnectionServiceHookMetaData();

  if (Boolean(error)) {
    const errorDescription =
      typeof error === "object" && error !== null
        ? JSON.stringify(error, undefined, 2) // Safely stringify the object with pretty-printing
        : String(error ?? ""); // Convert other types (e.g., numbers, strings) to string

    toaster.create({
      description: `Connection Type Meta request failed. Error: ${errorDescription}`,
      title: "Failed to retrieve Connection Type Meta",
      type: "error",
    });
  }

  const formattedData: Record<string, ConnectionMetaEntry> = {};
  const keysList: Array<string> = [];

  data?.forEach((item) => {
    const key = item.connection_type;

    keysList.push(key);

    // Ensure standard_fields has required properties
    const defaultStandardFields: StandardFieldSpec | undefined = {
      description: { hidden: false, placeholder: undefined, title: "Description" },
      host: { hidden: false, placeholder: undefined, title: "Host" },
      login: { hidden: false, placeholder: undefined, title: "Login" },
      password: { hidden: false, placeholder: undefined, title: "Password" },
      port: { hidden: false, placeholder: undefined, title: "Port" },
      url_schema: { hidden: false, placeholder: undefined, title: "Schema" },
    };

    const mergeWithDefaults = (
      defaultFields: StandardFieldSpec,
      customFields?: StandardFieldSpec,
    ): StandardFieldSpec =>
      Object.keys(defaultFields).reduce<StandardFieldSpec>((acc, newKey) => {
        const defaultValue = defaultFields[newKey];
        const customValue = customFields?.[newKey];

        acc[newKey] =
          customValue && typeof customValue === "object"
            ? {
                ...defaultValue,
                ...customValue,
              }
            : { ...defaultValue };

        return acc;
      }, {});

    const populatedStandardFields: StandardFieldSpec = mergeWithDefaults(
      defaultStandardFields,
      item.standard_fields,
    );

    if (populatedStandardFields.url_schema) {
      if (!populatedStandardFields.schema) {
        populatedStandardFields.schema = populatedStandardFields.url_schema;
      }
      delete populatedStandardFields.url_schema;
    }

    formattedData[key] = {
      ...item,
      standard_fields: populatedStandardFields,
    };
  });

  return { formattedData, keysList };
};
