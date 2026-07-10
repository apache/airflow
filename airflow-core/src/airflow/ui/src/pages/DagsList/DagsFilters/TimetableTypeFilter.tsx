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
import { CloseButton, Input, InputGroup } from "@chakra-ui/react";
import { useEffect, useRef, useState, type ChangeEvent } from "react";
import { useTranslation } from "react-i18next";
import { FiClock } from "react-icons/fi";
import { useDebouncedCallback } from "use-debounce";

const debounceDelay = 200;

type Props = {
  readonly onChange: (value: string) => void;
  readonly value: string;
};

export const TimetableTypeFilter = ({ onChange, value: initialValue }: Props) => {
  const [value, setValue] = useState(initialValue);
  const lastSentValue = useRef(initialValue);
  const { t: translate } = useTranslation("dags");

  const handleFilterChange = useDebouncedCallback((newValue: string) => {
    lastSentValue.current = newValue;
    onChange(newValue);
  }, debounceDelay);

  useEffect(() => {
    if (initialValue !== lastSentValue.current) {
      setValue(initialValue);
      lastSentValue.current = initialValue;
    }
  }, [initialValue]);

  const onInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    const newValue = event.target.value;

    setValue(newValue);
    handleFilterChange(newValue.trim());
  };

  const clearFilter = () => {
    handleFilterChange.cancel();
    lastSentValue.current = "";
    setValue("");
    onChange("");
  };

  return (
    <InputGroup
      colorPalette="brand"
      endElement={
        Boolean(value) ? (
          <CloseButton
            aria-label={translate("filters.timetableTypeClear")}
            data-testid="clear-timetable-type-filter"
            onClick={clearFilter}
            size="xs"
          />
        ) : undefined
      }
      maxWidth="260px"
      minWidth="220px"
      startElement={<FiClock />}
    >
      <Input
        aria-label={translate("filters.timetableType")}
        data-testid="dags-timetable-type-filter"
        onChange={onInputChange}
        placeholder={translate("filters.timetableType")}
        value={value}
      />
    </InputGroup>
  );
};
