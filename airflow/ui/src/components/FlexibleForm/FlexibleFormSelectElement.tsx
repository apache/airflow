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
import type { ParamSchema, ParamSpec } from "src/queries/useDagParams";

import type { FlexibleFormElementProps } from ".";
import { FlexibleFormFieldAdvancedArray } from "./FlexibleFormFieldAdvancedArray";
import { FlexibleFormFieldBool } from "./FlexibleFormFieldBool";
import { FlexibleFormFieldDate } from "./FlexibleFormFieldDate";
import { FlexibleFormFieldDateTime } from "./FlexibleFormFieldDateTime";
import { FlexibleFormFieldDropdown } from "./FlexibleFormFieldDropdown";
import { FlexibleFormFieldMultiSelect } from "./FlexibleFormFieldMultiSelect";
import { FlexibleFormFieldMultilineText } from "./FlexibleFormFieldMultilineText";
import { FlexibleFormFieldNumber } from "./FlexibleFormFieldNumber";
import { FlexibleFormFieldObject } from "./FlexibleFormFieldObject";
import { FlexibleFormFieldString } from "./FlexibleFormFieldString";
import { FlexibleFormFieldStringArray } from "./FlexibleFormFieldStringArray";
import { FlexibleFormFieldTime } from "./FlexibleFormFieldTime";

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

const isFieldAdvancedArray = (fieldType: string, fieldSchema: ParamSchema) =>
  fieldType === "array" && fieldSchema.items?.type !== "string";

const isFieldBool = (fieldType: string) => fieldType === "boolean";

const isFieldDate = (fieldType: string, fieldSchema: ParamSchema) =>
  fieldType === "string" && fieldSchema.format === "date";

const isFieldDateTime = (fieldType: string, fieldSchema: ParamSchema) =>
  fieldType === "string" && fieldSchema.format === "date-time";

const enumTypes = ["string", "number", "integer"];

const isFieldDropdown = (fieldType: string, fieldSchema: ParamSchema) =>
  enumTypes.includes(fieldType) && Array.isArray(fieldSchema.enum);

const isFieldMultilineText = (fieldType: string, fieldSchema: ParamSchema) =>
  fieldType === "string" && fieldSchema.format === "multiline";

const isFieldMultiSelect = (fieldType: string, fieldSchema: ParamSchema) =>
  fieldType === "array" && Array.isArray(fieldSchema.examples);

const isFieldNumber = (fieldType: string) => {
  const numberTypes = ["integer", "number"];

  return numberTypes.includes(fieldType);
};

const isFieldObject = (fieldType: string) => fieldType === "object";

const isFieldStringArray = (fieldType: string, fieldSchema: ParamSchema) =>
  fieldType === "array" &&
  (!fieldSchema.items || fieldSchema.items.type === undefined || fieldSchema.items.type === "string");

const isFieldTime = (fieldType: string, fieldSchema: ParamSchema) =>
  fieldType === "string" && fieldSchema.format === "date";

export const FlexibleFormSelectElement = ({ key, name, param }: FlexibleFormElementProps) => {
  // FUTURE: Add support for other types as described in AIP-68 via Plugins
  const fieldType = inferType(param);

  if (isFieldBool(fieldType)) {
    return <FlexibleFormFieldBool key={key} name={name} param={param} />;
  } else if (isFieldDateTime(fieldType, param.schema)) {
    return <FlexibleFormFieldDateTime key={key} name={name} param={param} />;
  } else if (isFieldDate(fieldType, param.schema)) {
    return <FlexibleFormFieldDate key={key} name={name} param={param} />;
  } else if (isFieldTime(fieldType, param.schema)) {
    return <FlexibleFormFieldTime key={key} name={name} param={param} />;
  } else if (isFieldDropdown(fieldType, param.schema)) {
    return <FlexibleFormFieldDropdown key={key} name={name} param={param} />;
  } else if (isFieldMultiSelect(fieldType, param.schema)) {
    return <FlexibleFormFieldMultiSelect key={key} name={name} param={param} />;
  } else if (isFieldStringArray(fieldType, param.schema)) {
    return <FlexibleFormFieldStringArray key={key} name={name} param={param} />;
  } else if (isFieldAdvancedArray(fieldType, param.schema)) {
    return <FlexibleFormFieldAdvancedArray key={key} name={name} param={param} />;
  } else if (isFieldObject(fieldType)) {
    return <FlexibleFormFieldObject key={key} name={name} param={param} />;
  } else if (isFieldNumber(fieldType)) {
    return <FlexibleFormFieldNumber key={key} name={name} param={param} />;
  } else if (isFieldMultilineText(fieldType, param.schema)) {
    return <FlexibleFormFieldMultilineText key={key} name={name} param={param} />;
  } else {
    return <FlexibleFormFieldString key={key} name={name} param={param} />;
  }
};
