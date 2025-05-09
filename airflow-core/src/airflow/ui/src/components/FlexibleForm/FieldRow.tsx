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
import { Field, Stack } from "@chakra-ui/react";
import { useState } from "react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { FieldSelector } from "./FieldSelector";
import { isRequired } from "./isParamRequired";

/** Render a normal form row with a field that is auto-selected */
export const FieldRow = ({ name, onUpdate: rowOnUpdate }: FlexibleFormElementProps) => {
  const { paramsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;
  const [error, setError] = useState<unknown>(
    isRequired(param) && param.value === null ? "This field is required" : undefined,
  );
  const [isValid, setIsValid] = useState(!(isRequired(param) && param.value === null));

  // console.log(param);

  const onUpdate = (value?: string, _error?: unknown) => {
    if (Boolean(_error)) {
      setIsValid(false);
      setError(_error);
      rowOnUpdate(undefined, _error);
    } else if (isRequired(param) && (!Boolean(value) || value === "")) {
      setIsValid(false);
      setError("This field is required");
      rowOnUpdate(undefined, "This field is required");
    } else {
      setIsValid(true);
      setError(undefined);
      rowOnUpdate();
    }
  };

  return (
    <Field.Root invalid={!isValid} orientation="horizontal" required={isRequired(param)}>
      <Stack>
        <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
          {param.schema.title ?? name} <Field.RequiredIndicator />
        </Field.Label>
      </Stack>
      <Stack css={{ flexBasis: "70%" }}>
        <FieldSelector name={name} onUpdate={onUpdate} />
        {param.description === null ? (
          param.schema.description_md === undefined ? undefined : (
            <Field.HelperText>
              <Markdown remarkPlugins={[remarkGfm]}>{param.schema.description_md}</Markdown>
            </Field.HelperText>
          )
        ) : (
          <Field.HelperText>{param.description}</Field.HelperText>
        )}
        {isValid ? undefined : <Field.ErrorText>{String(error)}</Field.ErrorText>}
      </Stack>
    </Field.Root>
  );
};
