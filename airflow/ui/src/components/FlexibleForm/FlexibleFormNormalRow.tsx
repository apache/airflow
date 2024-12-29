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
import { Field, Stack } from "@chakra-ui/react";
import Markdown from "react-markdown";

import type { ParamSpec } from "src/queries/useDagParams";

import type { FlexibleFormElementProps } from ".";
import { FlexibleFormFieldBool, isFieldBool } from "./FlexibleFormFieldBool";
import { FlexibleFormFieldDate, isFieldDate } from "./FlexibleFormFieldDate";
import { FlexibleFormFieldDateTime, isFieldDateTime } from "./FlexibleFormFieldDateTime";
import { FlexibleFormFieldDropdown, isFieldDropdown } from "./FlexibleFormFieldDropdown";
import { FlexibleFormFieldString } from "./FlexibleFormFieldString";
import { FlexibleFormFieldTime, isFieldTime } from "./FlexibleFormFieldTime";

const isRequired = (param: ParamSpec) =>
  // The field is required if the schema type is defined.
  // But if the type "null" is included, then the field is not required.
  // We assume that "null" is only defined if the type is an array.
  Boolean(param.schema.type) && (!Array.isArray(param.schema.type) || !param.schema.type.includes("null"));

const inferType = (param: ParamSpec) => {
  if (Boolean(param.schema.type)) {
    // If there are multiple types, we assume that the first one is the correct one that is not "null".
    // "null" is only used to signal the value is optional.
    if (Array.isArray(param.schema.type)) {
      return param.schema.type.find((type) => type !== "null") ?? "string";
    }

    return param.schema.type ?? "string";
  }

  // If the type is not defined, we infer it from the value.
  if (Array.isArray(param.value)) {
    return "array";
  }

  return typeof param.value;
};

export const FlexibleFormSelectElement = ({ key, name, param }: FlexibleFormElementProps) => {
  // FUTURE: Add support for other types as described in AIP-68 via Plugins
  const fieldType = inferType(param);

  if (isFieldBool(fieldType)) {
    return <FlexibleFormFieldBool key={key} name={name} param={param} />;
  } else if (isFieldDateTime(fieldType, param.schema.format)) {
    return <FlexibleFormFieldDateTime key={key} name={name} param={param} />;
  } else if (isFieldDate(fieldType, param.schema.format)) {
    return <FlexibleFormFieldDate key={key} name={name} param={param} />;
  } else if (isFieldTime(fieldType, param.schema.format)) {
    return <FlexibleFormFieldTime key={key} name={name} param={param} />;
  } else if (isFieldDropdown(fieldType, param.schema.enum)) {
    return <FlexibleFormFieldDropdown key={key} name={name} param={param} />;
  } else {
    // TODO other elements like number, integer, select etc.
    // Missing:
    // - (Simple) Array as Textarea
    // - Array (as JSON via CodeMirror)
    // - Multiple Select
    // - Object (as JSON via CodeMirror)
    // - Number (Into or generic number input)
    // - Multiline Text
    // see airflow/www/templates/airflow/trigger.html for logic in Airflow 2
    return <FlexibleFormFieldString key={key} name={name} param={param} />;
  }
};

/** Render a normal form row with a field that is auto-selected */
export const FlexibleFormNormalRow = ({ key, name, param }: FlexibleFormElementProps) => (
  <Field.Root orientation="horizontal" required={isRequired(param)}>
    <Stack css={{ "flex-basis": "30%" }}>
      <Field.Label css={{ "flex-basis": "0" }} fontSize="md">
        {param.schema.title ?? name} <Field.RequiredIndicator />
      </Field.Label>
    </Stack>
    <Stack css={{ "flex-basis": "70%" }}>
      <FlexibleFormSelectElement key={key} name={name} param={param} />
      <Field.HelperText>
        {param.description ?? <Markdown>{param.schema.description_md}</Markdown>}
      </Field.HelperText>
    </Stack>
  </Field.Root>
);
