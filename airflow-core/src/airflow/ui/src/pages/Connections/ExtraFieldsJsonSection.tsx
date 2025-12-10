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
import { Accordion, Field, Span } from "@chakra-ui/react";
import type { Control } from "react-hook-form";
import { Controller } from "react-hook-form";
import { useTranslation } from "react-i18next";

import { JsonEditor } from "src/components/JsonEditor";

import type { ConnectionBody } from "./Connections";

type ExtraFieldsJsonSectionProps = {
  readonly control: Control<ConnectionBody>;
  readonly error?: string;
  readonly isEditMode: boolean;
  readonly onBlur: (value: string) => string;
};

export const ExtraFieldsJsonSection = ({
  control,
  error,
  isEditMode,
  onBlur,
}: ExtraFieldsJsonSectionProps) => {
  const { t: translate } = useTranslation("admin");

  return (
    <Accordion.Item key="extraJson" value="extraJson">
      <Accordion.ItemTrigger cursor="button">
        <Span flex="1">{translate("connections.form.extraFieldsJson")}</Span>
        <Accordion.ItemIndicator />
      </Accordion.ItemTrigger>
      <Accordion.ItemContent>
        <Accordion.ItemBody>
          <Controller
            control={control}
            name="extra"
            render={({ field }) => (
              <Field.Root invalid={Boolean(error)}>
                <JsonEditor
                  {...field}
                  onBlur={() => {
                    field.onChange(onBlur(field.value));
                  }}
                />
                {Boolean(error) ? <Field.ErrorText>{error}</Field.ErrorText> : undefined}
                {isEditMode ? (
                  <Field.HelperText>
                    {translate("connections.form.helperTextForRedactedFields")}
                  </Field.HelperText>
                ) : undefined}
              </Field.Root>
            )}
          />
        </Accordion.ItemBody>
      </Accordion.ItemContent>
    </Accordion.Item>
  );
};
