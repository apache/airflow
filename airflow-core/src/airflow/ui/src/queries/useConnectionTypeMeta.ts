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

import { useConnectionServiceHookMetaData } from "openapi/queries";
import type {
  ConnectionHookFieldBehavior,
  ConnectionHookMetaData,
  StandardHookFields,
} from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

import type { ParamsSpec } from "./useDagParams";

type StandardFieldSchema = {
  hidden?: boolean | undefined;
  placeholder?: string | undefined;
  title?: string | undefined;
};

export type StandardFieldSpec = Record<string, StandardFieldSchema>;

export type ConnectionMetaEntry = {
  connection_type: string;
  default_conn_name: string | undefined;
  extra_fields: ParamsSpec;
  hook_class_name: string;
  hook_name: string;
  standard_fields: StandardFieldSpec | undefined;
};

/**
 * Type guard to check if an item has a valid (non-null) connection_type
 */
const hasValidConnectionType = (
  item: ConnectionHookMetaData,
): item is { connection_type: string } & ConnectionHookMetaData => item.connection_type !== null;

/**
 * Convert a single ConnectionHookFieldBehavior to StandardFieldSchema
 */
const convertField = (
  fieldBehavior: ConnectionHookFieldBehavior | null,
  defaultValue: StandardFieldSchema,
): StandardFieldSchema => {
  if (fieldBehavior === null) {
    return { ...defaultValue };
  }

  return {
    hidden: fieldBehavior.hidden ?? defaultValue.hidden,
    placeholder: fieldBehavior.placeholder ?? defaultValue.placeholder,
    title: fieldBehavior.title ?? defaultValue.title,
  };
};

/**
 * Convert StandardHookFields from API to StandardFieldSpec
 */
const convertStandardFields = (
  apiFields: StandardHookFields | null,
  defaultFields: StandardFieldSpec,
): StandardFieldSpec => {
  if (apiFields === null) {
    return { ...defaultFields };
  }

  const result: StandardFieldSpec = {
    description: convertField(apiFields.description, defaultFields.description ?? {}),
    host: convertField(apiFields.host, defaultFields.host ?? {}),
    login: convertField(apiFields.login, defaultFields.login ?? {}),
    password: convertField(apiFields.password, defaultFields.password ?? {}),
    port: convertField(apiFields.port, defaultFields.port ?? {}),
    // Convert url_schema to schema for the form
    schema: convertField(apiFields.url_schema, defaultFields.url_schema ?? {}),
  };

  return result;
};

export const useConnectionTypeMeta = () => {
  const { t: translate } = useTranslation("admin");
  const { data, error, isPending } = useConnectionServiceHookMetaData();

  if (Boolean(error)) {
    const errorDescription =
      typeof error === "object" && error !== null
        ? JSON.stringify(error, undefined, 2) // Safely stringify the object with pretty-printing
        : String(Boolean(error) ? error : ""); // Convert other types (e.g., numbers, strings) to string

    toaster.create({
      description: errorDescription,
      title: translate("admin:connections.typeMeta.error"),
      type: "error",
    });
  }

  const formattedData: Record<string, ConnectionMetaEntry> = {};
  const hookNames: Record<string, string> = {};
  const keysList: Array<string> = [];

  const defaultStandardFields: StandardFieldSpec = {
    description: {
      hidden: false,
      placeholder: undefined,
      title: translate("admin:connections.typeMeta.standardFields.description"),
    },
    host: {
      hidden: false,
      placeholder: undefined,
      title: translate("admin:connections.typeMeta.standardFields.host"),
    },
    login: {
      hidden: false,
      placeholder: undefined,
      title: translate("admin:connections.typeMeta.standardFields.login"),
    },
    password: {
      hidden: false,
      placeholder: undefined,
      title: translate("admin:connections.typeMeta.standardFields.password"),
    },
    port: {
      hidden: false,
      placeholder: undefined,
      title: translate("admin:connections.typeMeta.standardFields.port"),
    },
    url_schema: {
      hidden: false,
      placeholder: undefined,
      title: translate("admin:connections.typeMeta.standardFields.url_schema"),
    },
  };

  // Filter to only items with valid connection_type and transform
  const validItems = data?.filter(hasValidConnectionType) ?? [];

  validItems.forEach((item) => {
    const key = item.connection_type;

    hookNames[key] = item.hook_name;
    keysList.push(key);

    const populatedStandardFields = convertStandardFields(item.standard_fields, defaultStandardFields);

    const entry: ConnectionMetaEntry = {
      connection_type: item.connection_type,
      default_conn_name: item.default_conn_name ?? undefined,
      extra_fields: (item.extra_fields ?? {}) as ParamsSpec,
      hook_class_name: item.hook_class_name ?? "",
      hook_name: item.hook_name,
      standard_fields: populatedStandardFields,
    };

    formattedData[key] = entry;
  });

  keysList.sort((first, second) => (hookNames[first] ?? first).localeCompare(hookNames[second] ?? second));

  return { formattedData, hookNames, isPending, keysList };
};
