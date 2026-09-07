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
import { useState } from "react";

import { Input, InputGroup } from "@chakra-ui/react";

import { PasswordToggle } from "src/components/PasswordToggle";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

export const FieldPassword = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const [showPassword, setShowPassword] = useState(false);
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;
  const handleChange = (value: string) => {
    if (paramsDict[name]) {
      // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
      paramsDict[name].value = value === "" ? null : value;
    }

    setParamsDict(paramsDict);
    onUpdate(value);
  };

  return (
    <InputGroup
      endElement={<PasswordToggle isVisible={showPassword} onToggle={() => setShowPassword(!showPassword)} />}
    >
      <Input
        autoComplete="new-password"
        disabled={disabled}
        id={`element_${name}`}
        maxLength={param.schema.maxLength ?? undefined}
        minLength={param.schema.minLength ?? undefined}
        name={`element_${name}`}
        onChange={(event) => {
          handleChange(event.target.value);
        }}
        size="sm"
        type={showPassword ? "text" : "password"}
        value={(param.value ?? "") as string}
      />
    </InputGroup>
  );
};
