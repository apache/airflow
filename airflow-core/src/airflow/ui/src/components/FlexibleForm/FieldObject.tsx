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
import { JsonEditor } from "../JsonEditor";

export const FieldObject = ({ name, onUpdate }: FlexibleFormElementProps) => {
  const { paramsDict, setParamsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;

  const handleChange = (value: string) => {
    try {
      // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
      // eslint-disable-next-line unicorn/no-null
      const parsedValue = value === "" ? null : (JSON.parse(value) as JSON);

      if (paramsDict[name]) {
        paramsDict[name].value = parsedValue;
      }

      setParamsDict(paramsDict);
      onUpdate(value);
    } catch (_error) {
      onUpdate("", _error);
    }
  };

  return (
    <JsonEditor
      id={`element_${name}`}
      onChange={handleChange}
      value={JSON.stringify(param.value ?? [], undefined, 2)}
    />
  );
};
