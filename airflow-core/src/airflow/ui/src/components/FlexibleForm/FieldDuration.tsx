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
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import { useTranslation } from "react-i18next";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

dayjs.extend(duration);

export const FieldDuration = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;
  const handleChange = (value: string) => {
    const isEmpty = value === "";
    const parsedValue = value.replaceAll(",", ".");

    if (paramsDict[name]) {
      setParamsDict({
        ...paramsDict,
        [name]: {
          ...paramsDict[name],
          value: isEmpty ? null : parsedValue,
        },
      });
    }

    if (isEmpty) {
      onUpdate(parsedValue);

      return;
    }
    const dur = dayjs.duration(parsedValue);
    const isValid = !Number.isNaN(dur.asMilliseconds());

    if (isValid) {
      onUpdate(parsedValue);
    } else {
      onUpdate(undefined, translate("flexibleForm.validationErrorDuration"));
    }
  };

  return (
    <Input
      disabled={disabled}
      id={`element_${name}`}
      name={`element_${name}`}
      onChange={(event) => {
        handleChange(event.target.value);
      }}
      placeholder={translate("flexibleForm.durationPlaceholder")}
      size="sm"
      value={(param.value ?? "") as string}
    />
  );
};
