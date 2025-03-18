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
import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { FieldAdvancedArray } from "./FieldAdvancedArray";
import { FieldBool } from "./FieldBool";
import { FieldDateTime } from "./FieldDateTime";
import { FieldDropdown } from "./FieldDropdown";
import { FieldMultiSelect } from "./FieldMultiSelect";
import { FieldMultilineText } from "./FieldMultilineText";
import { FieldNumber } from "./FieldNumber";
import { FieldObject } from "./FieldObject";
import { FieldString } from "./FieldString";
import { FieldStringArray } from "./FieldStringArray";

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
  fieldType === "string" && fieldSchema.format === "time";

export const FieldSelector = ({ name }: FlexibleFormElementProps) => {
  // FUTURE: Add support for other types as described in AIP-68 via Plugins
  const { initialParamDict } = useParamStore();
  const param = initialParamDict[name] ?? paramPlaceholder;
  const fieldType = inferType(param);

  if (isFieldBool(fieldType)) {
    return <FieldBool name={name} />;
  } else if (isFieldDateTime(fieldType, param.schema)) {
    return <FieldDateTime name={name} type="datetime-local" />;
  } else if (isFieldDate(fieldType, param.schema)) {
    return <FieldDateTime name={name} type="date" />;
  } else if (isFieldTime(fieldType, param.schema)) {
    return <FieldDateTime name={name} type="time" />;
  } else if (isFieldDropdown(fieldType, param.schema)) {
    return <FieldDropdown name={name} />;
  } else if (isFieldMultiSelect(fieldType, param.schema)) {
    return <FieldMultiSelect name={name} />;
  } else if (isFieldStringArray(fieldType, param.schema)) {
    return <FieldStringArray name={name} />;
  } else if (isFieldAdvancedArray(fieldType, param.schema)) {
    return <FieldAdvancedArray name={name} />;
  } else if (isFieldObject(fieldType)) {
    return <FieldObject name={name} />;
  } else if (isFieldNumber(fieldType)) {
    return <FieldNumber name={name} />;
  } else if (isFieldMultilineText(fieldType, param.schema)) {
    return <FieldMultilineText name={name} />;
  } else {
    return <FieldString name={name} />;
  }
};
