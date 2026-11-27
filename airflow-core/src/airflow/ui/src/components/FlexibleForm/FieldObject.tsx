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
import { useContext, useEffect, useState } from "react";

import { NativeSelect, Textarea } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { parseDocument, stringify } from "yaml";

import { AllowYamlObjectInput } from "src/context/AllowYamlObjectInput";
import type { ParamValue } from "src/queries/useDagParams";
import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { JsonEditor } from "../JsonEditor";

const parseYamlObject = (text: string, translate: TFunction<"components">): ParamValue => {
  const document = parseDocument(text, { stringKeys: true });
  const problem = document.errors[0] ?? document.warnings[0];

  if (problem) {
    throw problem;
  }
  const value = document.toJS({
    reviver: (_key, entry: unknown) => {
      if (typeof entry === "number" && !Number.isFinite(entry)) {
        throw new TypeError(translate("flexibleForm.validationErrorYamlNumber"));
      }

      return entry;
    },
  }) as ParamValue;

  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(translate("flexibleForm.validationErrorYamlObject"));
  }

  return value;
};

export const FieldObject = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const allowYaml = useContext(AllowYamlObjectInput);
  const { t: translate } = useTranslation("components");
  const [format, setFormat] = useState("json");
  const [draft, setDraft] = useState<{ invalid: boolean; text: string; value: ParamValue }>();
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;

  const currentDraft = draft?.value === param.value ? draft : undefined;

  // Selecting a recent configuration can replace this field while its editor contains invalid text.
  useEffect(() => {
    if (draft?.invalid && draft.value !== param.value) {
      setDraft(undefined);
      onUpdate(param.value === null ? "" : JSON.stringify(param.value));
    }
  }, [draft, onUpdate, param.value]);

  const handleChange = (value: string, inputFormat = format) => {
    try {
      // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
      let parsedValue: ParamValue = null;

      if (value !== "") {
        parsedValue =
          inputFormat === "yaml" ? parseYamlObject(value, translate) : (JSON.parse(value) as ParamValue);
      }
      setParamsDict({ ...paramsDict, [name]: { ...param, value: parsedValue } });
      setDraft({ invalid: false, text: value, value: parsedValue });
      onUpdate(value);
    } catch (error) {
      setDraft({ invalid: true, text: value, value: param.value });
      onUpdate("", error instanceof Error ? error.message : error);
    }
  };

  return (
    <>
      {allowYaml ? (
        <NativeSelect.Root disabled={disabled} size="xs" width="fit-content">
          <NativeSelect.Field
            aria-label={translate("flexibleForm.objectInputFormat", { name: param.schema.title ?? name })}
            onChange={(event) => {
              setFormat(event.target.value);
              if (currentDraft?.invalid) {
                handleChange(currentDraft.text, event.target.value);
              } else {
                setDraft(undefined);
              }
            }}
            value={format}
          >
            <option value="json">JSON</option>
            <option value="yaml">YAML</option>
          </NativeSelect.Field>
          <NativeSelect.Indicator />
        </NativeSelect.Root>
      ) : undefined}
      {format === "yaml" ? (
        <Textarea
          aria-label={param.schema.title ?? name}
          disabled={disabled}
          fontFamily="mono"
          id={`element_${name}`}
          onChange={(event) => handleChange(event.target.value)}
          rows={7}
          spellCheck={false}
          value={currentDraft?.text ?? stringify(param.value ?? {})}
        />
      ) : (
        <JsonEditor
          editable={!disabled}
          id={`element_${name}`}
          onChange={handleChange}
          value={
            (allowYaml ? currentDraft?.text : undefined) ?? JSON.stringify(param.value ?? {}, undefined, 2)
          }
        />
      )}
    </>
  );
};
