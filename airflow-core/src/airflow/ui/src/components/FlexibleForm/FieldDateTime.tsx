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

export const FieldDateTime = ({ name, ...rest }: FlexibleFormElementProps & InputProps) => {
  const { paramsDict, setParamsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;
  const handleChange = (value: string) => {
    if (paramsDict[name]) {
      if (rest.type === "datetime-local") {
        // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
        // eslint-disable-next-line unicorn/no-null
        paramsDict[name].value = value === "" ? null : `${value}:00+00:00`; // Need to suffix to make it UTC like
      } else {
        // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
        // eslint-disable-next-line unicorn/no-null
        paramsDict[name].value = value === "" ? null : value;
      }
    }

    setParamsDict(paramsDict);
  };

  return (
    <Input
      id={`element_${name}`}
      name={`element_${name}`}
      onChange={(event) => handleChange(event.target.value)}
      size="sm"
      type={rest.type}
      value={((param.value ?? "") as string).slice(0, 16)}
    />
  );
};
