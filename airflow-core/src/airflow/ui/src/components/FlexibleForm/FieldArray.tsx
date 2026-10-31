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
import { useState, type ClipboardEvent, type KeyboardEvent } from "react";

import { CreatableSelect, type MultiValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { parseArrayElement, type ArrayElement } from "./parseArrayElement";

type ChipOption = { index: number; label: string; value: ArrayElement };

const toChip = (value: ArrayElement, index: number): ChipOption => ({ index, label: String(value), value });

const getChipIdentity = (chip: ChipOption): string => String(chip.index);

export const FieldArray = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const [inputValue, setInputValue] = useState("");
  const param = paramsDict[name] ?? paramPlaceholder;
  const itemsType = param.schema.items?.type;

  const elements: Array<ArrayElement> = Array.isArray(param.value)
    ? (param.value as Array<ArrayElement>)
    : [];

  const setElements = (newElements: Array<ArrayElement>) => {
    if (paramsDict[name]) {
      // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
      paramsDict[name].value = newElements.length ? newElements : null;
    }
    setParamsDict(paramsDict);
  };

  const addEntries = (rawText: string) => {
    const entries = rawText.split("\n").filter((entry) => entry.trim() !== "");
    const coerced = entries.map((entry) => ({ entry, value: parseArrayElement(entry, itemsType) }));
    const invalid = coerced.filter(({ value }) => value === undefined).map(({ entry }) => entry.trim());
    const valid = coerced
      .map(({ value }) => value)
      .filter((value): value is ArrayElement => value !== undefined);
    const newElements = [...elements, ...valid];

    if (valid.length) {
      setElements(newElements);
    }
    if (invalid.length) {
      onUpdate(
        undefined,
        translate("flexibleForm.validationErrorArrayElement", {
          type: typeof itemsType === "string" ? itemsType : "string",
          values: invalid.join(", "),
        }),
      );
    } else {
      onUpdate(String(newElements));
    }
  };

  const commitInput = () => {
    if (inputValue.trim() !== "") {
      addEntries(inputValue);
      setInputValue("");
    }
  };

  const handleKeyDown = (event: KeyboardEvent) => {
    if ((event.key === "Enter" || event.key === "Tab") && inputValue.trim() !== "") {
      event.preventDefault();
      commitInput();
    }
  };

  const handlePaste = (event: ClipboardEvent) => {
    const text = event.clipboardData.getData("text");

    if (text.includes("\n")) {
      event.preventDefault();
      addEntries(text);
      setInputValue("");
    }
  };

  const handleChange = (chips: MultiValue<ChipOption>) => {
    const newElements = chips.map((chip) => chip.value);

    setElements(newElements);
    onUpdate(String(newElements));
  };

  return (
    <div onPasteCapture={handlePaste}>
      <CreatableSelect
        aria-label={translate("flexibleForm.placeholderArray")}
        chakraStyles={{
          // Optical centering: the icon reads as sitting high next to lowercase labels without the nudge.
          multiValueRemove: (provided) => ({
            ...provided,
            alignSelf: "center",
            transform: "translateY(1px)",
          }),
        }}
        components={{ DropdownIndicator: null }}
        getOptionValue={getChipIdentity}
        id={`element_${name}`}
        inputValue={inputValue}
        isClearable
        isDisabled={disabled}
        isMulti
        menuIsOpen={false}
        name={`element_${name}`}
        onBlur={commitInput}
        onChange={handleChange}
        onInputChange={setInputValue}
        onKeyDown={handleKeyDown}
        placeholder={translate("flexibleForm.placeholderArray")}
        size="sm"
        value={elements.map(toChip)}
      />
    </div>
  );
};
