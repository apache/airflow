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
import { createListCollection } from "@chakra-ui/react/collection";
import { useEffect, useMemo, useRef } from "react";
import { useTranslation } from "react-i18next";

import { Select } from "src/components/ui";
import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

const labelLookup = (key: string, valuesDisplay: Record<string, string> | undefined): string => {
  if (valuesDisplay && typeof valuesDisplay === "object") {
    return valuesDisplay[key] ?? key;
  }

  return key;
};
const enumTypes = ["string", "number", "integer"];

export const FieldDropdown = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;

  // --- Cascading params: compute effective enum options (#61594) ---
  const parentRawValue = param.schema.depends_on ? paramsDict[param.schema.depends_on]?.value : undefined;
  const parentValue = parentRawValue !== undefined
    ? String(Array.isArray(parentRawValue) ? parentRawValue[0] : (parentRawValue ?? ""))
    : "";

  const effectiveEnum = useMemo(() => {
    const dependsOn = param.schema.depends_on;
    const optionsMap = param.schema.options_map;

    // No cascading config — use original enum from schema
    if (!dependsOn || !optionsMap) {
      return param.schema.enum;
    }

    if (parentValue && parentValue in optionsMap) {
      return optionsMap[parentValue];
    }

    // Fallback: show all child options when parent value is empty or unrecognized
    return [...new Set(Object.values(optionsMap).flat())];
  }, [param.schema.depends_on, param.schema.options_map, param.schema.enum, parentValue]);

  // --- Cascading params: auto-reset child value when it becomes invalid (#61594) ---
  useEffect(() => {
    if (!param.schema.depends_on || !param.schema.options_map || !effectiveEnum) {
      return;
    }

    const currentValue = Array.isArray(param.value) ? param.value[0] : param.value;
    const isValid = effectiveEnum.some((opt) => String(opt) === String(currentValue));

    if (!isValid && effectiveEnum.length > 0 && paramsDict[name]) {
      // eslint-disable-next-line unicorn/no-null
      paramsDict[name].value = effectiveEnum[0] ?? null;
      setParamsDict({ ...paramsDict });
    }
  }, [effectiveEnum, param.value, param.schema.depends_on, param.schema.options_map, name, paramsDict, setParamsDict]);

  const selectOptions = createListCollection({
    items:
      effectiveEnum?.map((value) => ({
        label: labelLookup(value, param.schema.values_display),
        value,
      })) ?? [],
  });

  const contentRef = useRef<HTMLDivElement | null>(null);

  const handleChange = ([value]: Array<string>) => {
    if (paramsDict[name]) {
      // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
      // eslint-disable-next-line unicorn/no-null
      paramsDict[name].value = value ?? null;
    }

    setParamsDict({ ...paramsDict });
    onUpdate(value);
  };

  return (
    <Select.Root
      collection={selectOptions}
      disabled={disabled}
      id={`element_${name}`}
      name={`element_${name}`}
      onValueChange={(event) => handleChange(event.value)}
      ref={contentRef}
      size="sm"
      value={enumTypes.includes(typeof param.value) ? [param.value as string] : undefined}
    >
      <Select.Trigger clearable>
        <Select.ValueText placeholder={translate("flexibleForm.placeholder")} />
      </Select.Trigger>
      <Select.Content portalRef={contentRef}>
        {selectOptions.items.map((option) => (
          <Select.Item item={option} key={option.value}>
            {option.label}
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
