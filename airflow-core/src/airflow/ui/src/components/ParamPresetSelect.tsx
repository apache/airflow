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

import { Field } from "@chakra-ui/react";
import { type SingleValue, Select as ReactSelect } from "chakra-react-select";
import { useTranslation } from "react-i18next";

import type { ParamPresets, ParamsSpec } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";

type Props = {
  readonly paramPresets?: ParamPresets;
  readonly paramsDict: ParamsSpec;
};

type Option = { label: string; value: string };

/**
 * Drop-down of the Dag author's named param presets.
 *
 * Picking one loads the Dag defaults and then applies the preset's values on top, so the same
 * preset always produces the same run config no matter what was edited before it was picked.
 */
export const ParamPresetSelect = ({ paramPresets, paramsDict }: Props) => {
  const { t: translate } = useTranslation("components");
  const { disabled, setConf } = useParamStore();
  const [selected, setSelected] = useState<Option | null>(null);

  const presetNames = Object.keys(paramPresets ?? {});

  if (presetNames.length === 0) {
    return undefined;
  }

  const options = presetNames.map((name) => ({ label: name, value: name }));

  // Not clearable: clearing would only blank the label, and resetting the form from an "x" would
  // throw away edits the user cannot get back.
  const handleChange = (option: SingleValue<Option>) => {
    if (!option) {
      return;
    }

    setSelected(option);

    const defaults = Object.fromEntries(Object.entries(paramsDict).map(([key, { value }]) => [key, value]));

    setConf(JSON.stringify({ ...defaults, ...paramPresets?.[option.value] }, undefined, 2));
  };

  return (
    <Field.Root mb={4}>
      <Field.Label fontSize="md">{translate("configForm.paramPreset")}</Field.Label>
      <ReactSelect
        id="param-preset"
        isDisabled={disabled}
        menuPosition="fixed"
        name="param-preset"
        onChange={handleChange}
        options={options}
        placeholder={translate("configForm.paramPresetPlaceholder")}
        size="sm"
        value={selected}
      />
      <Field.HelperText>{translate("configForm.paramPresetHelp")}</Field.HelperText>
    </Field.Root>
  );
};
