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
import { Field, Spinner, Stack } from "@chakra-ui/react";
import { Select } from "chakra-react-select";
import type { Control } from "react-hook-form";
import { Controller } from "react-hook-form";
import { useTranslation } from "react-i18next";

import type { ConnectionBody } from "./Connections";

type ConnectionTypeOption = {
  label: string | undefined;
  value: string;
};

type ConnectionTypeFieldProps = {
  readonly control: Control<ConnectionBody>;
  readonly isLoading: boolean;
  readonly options: Array<ConnectionTypeOption>;
};

export const ConnectionTypeField = ({ control, isLoading, options }: ConnectionTypeFieldProps) => {
  const { t: translate } = useTranslation("admin");

  return (
    <Controller
      control={control}
      name="conn_type"
      render={({ field: { onChange, value }, fieldState }) => (
        <Field.Root invalid={Boolean(fieldState.error)} orientation="horizontal" required>
          <Stack>
            <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
              {translate("connections.columns.connectionType")} <Field.RequiredIndicator />
            </Field.Label>
          </Stack>
          <Stack css={{ flexBasis: "70%" }}>
            <Stack>
              {isLoading ? (
                <Spinner size="sm" style={{ left: "60%", position: "absolute", top: "20%" }} />
              ) : undefined}
              <Select
                {...Field}
                isDisabled={isLoading}
                onChange={(val) => onChange(val?.value)}
                options={options}
                placeholder={translate("connections.form.selectConnectionType")}
                value={options.find((type) => type.value === value)}
              />
            </Stack>
            <Field.HelperText>{translate("connections.form.helperText")}</Field.HelperText>
          </Stack>
        </Field.Root>
      )}
      rules={{
        required: translate("connections.form.connectionTypeRequired"),
      }}
    />
  );
};
