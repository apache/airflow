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

type ExtraFieldSpec = Record<string, ParamSpec>;

type StandardFieldSpec = Record<string, StandardFieldSchema>;

type StandardFieldSchema = {
    hidden: boolean,
    placeholder: string | undefined
    title: string | undefined,
}

type ConnectionMetaEntry = {
    connection_type: string,
    default_conn_name: string | undefined,
    extra_fields: ExtraFieldSpec | undefined,
    hook_class_name: string,
    hook_name: string,
    standard_fields : StandardFieldSpec | undefined,
}

type ConnectionMeta = Array<ConnectionMetaEntry>;

export const useConnectionTypeMeta = () => {
  const { data, error }: { data?: ConnectionMeta; error?: unknown } =
    useConnectionServiceHookMetaData();

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

    const formattedData: Record<string, Record<string, unknown>> = {};
    const keysList: Array<string> = [];

    data?.forEach(item => {
    const key = item.connection_type;

    keysList.push(key);

    // Ensure standard_fields has required properties
    const defaultStandardFields: StandardFieldSpec = {
        description: { hidden: false, placeholder: undefined, title: "description" },
        host: { hidden: false, placeholder: undefined, title: "host" },
        login: { hidden: false, placeholder: undefined, title: "login" },
        password: { hidden: false, placeholder: undefined, title: "password" },
        port: { hidden: false, placeholder: undefined, title: "port" },
        schema: { hidden: false, placeholder: undefined, title: "schema" }, // Ensure schema is included
    };

    // Merge existing standard_fields with default values
    const populatedStandardFields: StandardFieldSpec = { 
        ...defaultStandardFields, 
        ...item.standard_fields // Use empty object if undefined
      };
  
      // Rename url_schema to schema if present
      if (populatedStandardFields.url_schema) {
        populatedStandardFields.schema = populatedStandardFields.url_schema;
        delete populatedStandardFields.url_schema;
      }

    formattedData[key] = {
        ...item,
        standard_fields: populatedStandardFields,
    };
    });

  return { formattedData, keysList };
};
