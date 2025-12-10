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
import { Field, Input, Stack } from "@chakra-ui/react";
import type { Control } from "react-hook-form";
import { Controller } from "react-hook-form";
import { useTranslation } from "react-i18next";

import type { ConnectionBody } from "./Connections";

type ConnectionIdFieldProps = {
  readonly control: Control<ConnectionBody>;
  readonly isDisabled: boolean;
};

export const ConnectionIdField = ({ control, isDisabled }: ConnectionIdFieldProps) => {
  const { t: translate } = useTranslation("admin");

  return (
    <Controller
      control={control}
      name="connection_id"
      render={({ field, fieldState }) => (
        <Field.Root invalid={Boolean(fieldState.error)} orientation="horizontal" required>
          <Stack>
            <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
              {translate("connections.columns.connectionId")} <Field.RequiredIndicator />
            </Field.Label>
          </Stack>
          <Stack css={{ flexBasis: "70%" }}>
            <Input {...field} disabled={isDisabled} required size="sm" />
            {fieldState.error ? <Field.ErrorText>{fieldState.error.message}</Field.ErrorText> : undefined}
          </Stack>
        </Field.Root>
      )}
      rules={{
        required: translate("connections.form.connectionIdRequired"),
        validate: (value) =>
          value.trim() === "" ? translate("connections.form.connectionIdRequirement") : true,
      }}
    />
  );
};
