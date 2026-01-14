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
import { Field, Stack, Spinner } from "@chakra-ui/react";
import { Select } from "chakra-react-select";
import { useMemo } from "react";
import { type Control, Controller, type FieldValues, type Path } from "react-hook-form";
import { useTranslation } from "react-i18next";

import { useTeamsServiceListTeams } from "openapi/queries";

type Props<T extends FieldValues = FieldValues> = {
  readonly control: Control<T>;
};

export const TeamSelector = <T extends FieldValues = FieldValues>({ control }: Props<T>) => {
  const { data, isLoading } = useTeamsServiceListTeams({ orderBy: ["name"] });
  const options = useMemo(
    () =>
      (data?.teams ?? []).map((team: { name: string }) => ({
        label: team.name,
        value: team.name,
      })),
    [data],
  );

  const { t: translate } = useTranslation("components");

  return (
    <Controller
      control={control}
      name={"team_name" as Path<T>}
      render={({ field: { onChange, value }, fieldState }) => (
        <Field.Root invalid={Boolean(fieldState.error)} orientation="horizontal">
          <Stack>
            <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
              {translate("team.selector.label")}
            </Field.Label>
          </Stack>
          <Stack css={{ flexBasis: "70%" }}>
            <Stack>
              {isLoading ? (
                <Spinner size="sm" style={{ left: "60%", position: "absolute", top: "20%" }} />
              ) : undefined}
              <Select
                {...Field}
                isClearable
                isDisabled={isLoading}
                onChange={(val) => onChange(val?.value)}
                options={options}
                placeholder={translate("team.selector.placeHolder")}
                value={options.find((option) => option.value === value)}
              />
            </Stack>
            <Field.HelperText>{translate("team.selector.helperText")}</Field.HelperText>
          </Stack>
        </Field.Root>
      )}
    />
  );
};
