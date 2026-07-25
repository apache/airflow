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
import { Box, Field } from "@chakra-ui/react";
import { Select as ReactSelect, type SingleValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";

type Props = {
  readonly onChange: (timetableType: SingleValue<TimetableTypeOption>) => void;
  readonly onInputChange: (value: string) => void;
  readonly onMenuScrollToBottom: () => void;
  readonly onMenuScrollToTop: () => void;
  readonly timetableTypes: Array<string>;
  readonly value: string | undefined;
};

type TimetableTypeOption = {
  label: string;
  value: string;
};

export const TimetableTypeFilter = ({
  onChange,
  onInputChange,
  onMenuScrollToBottom,
  onMenuScrollToTop,
  timetableTypes,
  value,
}: Props) => {
  const { t: translate } = useTranslation("dags");

  return (
    <Box flex="0 1 300px" maxWidth="100%" width="300px">
      <Field.Root>
        <ReactSelect<TimetableTypeOption>
          aria-label={translate("filters.timetableType")}
          chakraStyles={{
            clearIndicator: (provided) => ({
              ...provided,
              color: "gray.fg",
            }),
            control: (provided) => ({
              ...provided,
              colorPalette: "brand",
            }),
            menu: (provided) => ({
              ...provided,
              zIndex: 2,
            }),
          }}
          isClearable
          noOptionsMessage={() => translate("filters.noTimetableTypesFound")}
          onChange={onChange}
          onInputChange={onInputChange}
          onMenuScrollToBottom={onMenuScrollToBottom}
          onMenuScrollToTop={onMenuScrollToTop}
          options={timetableTypes.map((timetableType) => ({
            label: timetableType,
            value: timetableType,
          }))}
          placeholder={translate("filters.timetableType")}
          value={value === undefined ? null : { label: value, value }}
        />
      </Field.Root>
    </Box>
  );
};
