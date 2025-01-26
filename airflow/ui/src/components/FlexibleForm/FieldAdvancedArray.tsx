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
import { Text } from "@chakra-ui/react";
import { json } from "@codemirror/lang-json";
import { githubLight, githubDark } from "@uiw/codemirror-themes-all";
import CodeMirror from "@uiw/react-codemirror";
import { useState } from "react";

import { useColorMode } from "src/context/colorMode";

import type { FlexibleFormElementProps } from ".";
import { paramPlaceholder, useParamStore } from "../TriggerDag/useParamStore";

export const FieldAdvancedArray = ({ name }: FlexibleFormElementProps) => {
  const { colorMode } = useColorMode();
  const { paramsDict, setParamsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;
  const [error, setError] = useState<unknown>(undefined);
  // Determine the expected type based on schema
  const expectedType = param.schema.items?.type ?? "object";

  const handleChange = (value: string) => {
    setError(undefined);
    if (value === "") {
      if (paramsDict[name]) {
        // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
        // eslint-disable-next-line unicorn/no-null
        paramsDict[name].value = null;
      }
      setParamsDict(paramsDict);
    } else {
      try {
        const parsedValue = JSON.parse(value) as unknown;

        if (!Array.isArray(parsedValue)) {
          throw new TypeError("Value must be an array.");
        }

        if (expectedType === "number" && !parsedValue.every((item) => typeof item === "number")) {
          // Ensure all elements in the array are numbers
          throw new TypeError("All elements in the array must be numbers.");
        } else if (
          expectedType === "object" &&
          !parsedValue.every((item) => typeof item === "object" && item !== null)
        ) {
          // Ensure all elements in the array are objects
          throw new TypeError("All elements in the array must be objects.");
        }

        if (paramsDict[name]) {
          paramsDict[name].value = parsedValue;
        }

        setParamsDict(paramsDict);
      } catch (_error) {
        setError(expectedType === "number" ? String(_error).replace("JSON", "Array") : _error);
      }
    }
  };

  return (
    <>
      <CodeMirror
        basicSetup={{
          autocompletion: true,
          bracketMatching: true,
          foldGutter: true,
          lineNumbers: true,
        }}
        extensions={[json()]}
        height="200px"
        id={`element_${name}`}
        onChange={handleChange}
        style={{
          border: "1px solid var(--chakra-colors-border)",
          borderRadius: "8px",
          outline: "none",
          padding: "2px",
          width: "100%",
        }}
        theme={colorMode === "dark" ? githubDark : githubLight}
        value={JSON.stringify(param.value ?? [], undefined, 2)}
      />
      {Boolean(error) ? (
        <Text color="red.solid" fontSize="xs">
          {String(error)}
        </Text>
      ) : undefined}
    </>
  );
};
