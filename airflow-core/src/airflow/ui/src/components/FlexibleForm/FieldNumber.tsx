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
import { useTranslation } from "react-i18next";

import { NumberInputField, NumberInputRoot } from "src/system-components";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { getParamRangeViolation } from "./getParamRangeViolation";

export const FieldNumber = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;
  const handleChange = (value: string) => {
    if (value === "") {
      // If input is cleared, set the value to null or undefined
      if (paramsDict[name]) {
        // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
        paramsDict[name].value = null;
      }

      setParamsDict(paramsDict);
      onUpdate(value);

      return;
    }

    // Convert the string to a number if a valid value is entered
    const numericValue = Number(value);

    if (paramsDict[name]) {
      paramsDict[name].value = numericValue;
    }

    setParamsDict(paramsDict);

    const violation = getParamRangeViolation(numericValue, param.schema);

    if (violation) {
      onUpdate(
        undefined,
        violation.kind === "minimum"
          ? translate("flexibleForm.validationErrorMinimum", { minimum: violation.minimum })
          : translate("flexibleForm.validationErrorMaximum", { maximum: violation.maximum }),
      );

      return;
    }

    onUpdate(value);
  };

  return (
    <NumberInputRoot
      allowMouseWheel
      // Keep the value the user typed: clamping it to the schema bounds would silently trigger
      // the Dag with a corrected value instead of rejecting the out-of-range input.
      clampValueOnBlur={false}
      disabled={disabled}
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
