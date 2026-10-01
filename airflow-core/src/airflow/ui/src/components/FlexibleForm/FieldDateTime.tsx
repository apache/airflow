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
import { Input, type InputProps } from "@chakra-ui/react";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { DateTimeInput } from "../DateTimeInput";

export const FieldDateTime = ({
  name,
  namespace = "default",
  onUpdate,
  type,
  ...rest
}: FlexibleFormElementProps & InputProps) => {
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;
  const isTime = type === "time";
  const handleChange = (value: string) => {
    // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
    if (paramsDict[name]) {
      paramsDict[name].value = value === "" ? null : value;
    }

    setParamsDict(paramsDict);
    onUpdate(value);
  };

  if (type === "datetime-local") {
    return (
      <DateTimeInput
        disabled={disabled}
        id={`element_${name}`}
        name={`element_${name}`}
        onChange={(event) => handleChange(event.target.value)}
        size="sm"
        value={typeof param.value === "string" ? param.value : ""}
      />
    );
  }

  return (
    <Input
      disabled={disabled}
      id={`element_${name}`}
      name={`element_${name}`}
      onChange={(event) => handleChange(event.target.value)}
      required={rest.required}
      size="sm"
      step={isTime ? 1 : undefined}
      type={type}
      // A non-string value (e.g. an object leaking in from a malformed param) must degrade to an
      // empty input rather than throw and take the whole form down.
      value={typeof param.value === "string" ? param.value.slice(0, 16) : ""}
    />
  );
};
