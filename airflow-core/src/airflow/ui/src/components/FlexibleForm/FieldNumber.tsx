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
import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { NumberInputField, NumberInputRoot } from "../ui/NumberInput";

export const FieldNumber = ({ name, onUpdate }: FlexibleFormElementProps) => {
  const { paramsDict, setParamsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;
  const handleChange = (value: string) => {
    if (value === "") {
      // If input is cleared, set the value to null or undefined
      if (paramsDict[name]) {
        // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
        // eslint-disable-next-line unicorn/no-null
        paramsDict[name].value = null;
      }
    } else {
      // Convert the string to a number if a valid value is entered
      if (paramsDict[name]) {
        paramsDict[name].value = Number(value);
      }
    }

    setParamsDict(paramsDict);
    onUpdate(value);
  };

  return (
    <NumberInputRoot
      allowMouseWheel
      id={`element_${name}`}
      max={param.schema.maximum ?? undefined}
      min={param.schema.minimum ?? undefined}
      name={`element_${name}`}
      onValueChange={(event) => handleChange(event.value)}
      size="sm"
      value={(param.value ?? "") as string}
    >
      <NumberInputField />
    </NumberInputRoot>
  );
};
