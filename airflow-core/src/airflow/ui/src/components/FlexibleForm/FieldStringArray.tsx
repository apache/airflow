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
import { useTranslation } from "react-i18next";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

export const FieldStringArray = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;

  const handleChange = (newValue: string) => {
    const newValueArray = newValue.split("\n");

    if (paramsDict[name]) {
      paramsDict[name].value = newValueArray;
    }

    setParamsDict(paramsDict);
    onUpdate(newValue);
  };

  const handleBlur = () => {
    const currentValue = paramsDict[name]?.value;

    if (Array.isArray(currentValue) && currentValue.length === 1 && currentValue[0] === "") {
      if (paramsDict[name]) {
        // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
        // eslint-disable-next-line unicorn/no-null
        paramsDict[name].value = null;
      }

      setParamsDict(paramsDict);
    }
  };

  const value = Array.isArray(param.value) ? param.value.join("\n") : [];

  return (
    <Textarea
      disabled={disabled}
      id={`element_${name}`}
      name={`element_${name}`}
      onBlur={handleBlur}
      onChange={(event) => handleChange(event.target.value)}
      placeholder={translate("flexibleForm.placeholderArray")}
      rows={6}
      size="sm"
      value={value}
    />
  );
};
