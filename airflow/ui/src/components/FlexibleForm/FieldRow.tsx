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
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";

import type { ParamSpec } from "src/queries/useDagParams";
import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { FieldSelector } from "./FieldSelector";

const isRequired = (param: ParamSpec) =>
  // The field is required if the schema type is defined.
  // But if the type "null" is included, then the field is not required.
  // We assume that "null" is only defined if the type is an array.
  Boolean(param.schema.type) && (!Array.isArray(param.schema.type) || !param.schema.type.includes("null"));

/** Render a normal form row with a field that is auto-selected */
export const FieldRow = ({ name }: FlexibleFormElementProps) => {
  const { paramsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;

  return (
    <Field.Root orientation="horizontal" required={isRequired(param)}>
      <Stack>
        <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
          {param.schema.title ?? name} <Field.RequiredIndicator />
        </Field.Label>
      </Stack>
      <Stack css={{ flexBasis: "70%" }}>
        <FieldSelector name={name} />
        {param.description === null ? (
          param.schema.description_md === undefined ? undefined : (
            <Field.HelperText>
              <Markdown remarkPlugins={[remarkGfm]}>{param.schema.description_md}</Markdown>
            </Field.HelperText>
          )
        ) : (
          <Field.HelperText>{param.description}</Field.HelperText>
        )}
      </Stack>
    </Field.Root>
  );
};
