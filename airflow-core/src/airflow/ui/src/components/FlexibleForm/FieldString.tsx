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
import { Input } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

export const FieldString = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;
  const handleChange = (value: string) => {
    if (paramsDict[name]) {
      // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
      // eslint-disable-next-line unicorn/no-null
      paramsDict[name].value = value === "" ? null : value;
    }

    setParamsDict(paramsDict);
    onUpdate(value);
  };

  return (
    <>
      <Input
        disabled={disabled}
        id={`element_${name}`}
        list={param.schema.examples ? `list_${name}` : undefined}
        maxLength={param.schema.maxLength ?? undefined}
        minLength={param.schema.minLength ?? undefined}
        name={`element_${name}`}
        onChange={(event) => {
          handleChange(event.target.value);
        }}
        placeholder={param.schema.examples ? translate("flexibleForm.placeholderExamples") : undefined}
        size="sm"
        value={(param.value ?? "") as string}
      />
      {param.schema.examples ? (
        <datalist id={`list_${name}`}>
          {param.schema.examples.map((example) => (
            <option key={example} value={example}>
              {example}
            </option>
          ))}
        </datalist>
      ) : undefined}
    </>
  );
};
