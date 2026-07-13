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
import { Textarea } from "@chakra-ui/react";
import { useState } from "react";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

const matchesSchemaType = (parsed: unknown, types: Array<string | undefined>): boolean => {
  const valueType = Array.isArray(parsed) ? "array" : parsed === null ? "null" : typeof parsed;

  return types.some(
    (type) =>
      type === valueType ||
      // JSON Schema "integer" is a subtype of "number"; exclude floats that JSON.parse accepts.
      (type === "integer" && valueType === "number" && Number.isInteger(parsed)),
  );
};

export const FieldMultiType = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;

  // Tracks raw user input so the textarea doesn't snap back to the last valid value while the
  // user is typing something invalid. Cleared on valid input so external updates (e.g. prefill) show through.
  const [inputText, setInputText] = useState<string | null>(null);

  const schemaTypes = Array.isArray(param.schema.type) ? param.schema.type : [param.schema.type];
  const nonNullTypes = schemaTypes.filter((type): type is string => Boolean(type));
  const stringIsAllowed = nonNullTypes.includes("string");

  const storedDisplay =
    param.value === null || param.value === undefined
      ? ""
      : typeof param.value === "string"
        ? param.value
        : JSON.stringify(param.value, undefined, 2);

  const displayValue = inputText ?? storedDisplay;

  const handleChange = (value: string) => {
    if (!paramsDict[name]) {
      onUpdate(value);

      return;
    }

    if (value === "") {
      // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
      paramsDict[name].value = null;
      setInputText(null);
      setParamsDict(paramsDict);
      onUpdate(value);

      return;
    }

    let resolved: unknown;
    let matched = false;

    try {
      const parsed = JSON.parse(value) as unknown;

      if (matchesSchemaType(parsed, nonNullTypes)) {
        resolved = parsed;
        matched = true;
      }
    } catch {
      // not valid JSON — fall through
    }

    // String in schema means raw text is always a valid fallback.
    if (!matched && stringIsAllowed) {
      resolved = value;
      matched = true;
    }

    if (matched) {
      paramsDict[name].value = resolved;
      setInputText(null);
      setParamsDict(paramsDict);
      onUpdate(value);
    } else {
      // Don't overwrite the last valid stored value; keep the typed text visible and signal the error.
      setInputText(value);
      onUpdate("", `Value must be one of: ${nonNullTypes.join(", ")}`);
    }
  };

  return (
    <Textarea
      disabled={disabled}
      id={`element_${name}`}
      name={`element_${name}`}
      onChange={(event) => handleChange(event.target.value)}
      rows={6}
      size="sm"
      value={displayValue}
    />
  );
};
