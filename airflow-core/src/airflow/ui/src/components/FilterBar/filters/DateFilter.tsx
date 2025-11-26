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
import dayjs from "dayjs";

import { DateTimeInput } from "src/components/DateTimeInput";

import { FilterPill } from "../FilterPill";
import type { FilterPluginProps } from "../types";
import { isValidFilterValue } from "../utils";

export const DateFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const hasValue = isValidFilterValue(filter.config.type, filter.value);
  const displayValue =
    hasValue && typeof filter.value === "string" ? dayjs(filter.value).format("MMM DD, YYYY, hh:mm A") : "";

  const handleDateChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { value } = event.target;

    onChange(value || undefined);
  };

  return (
    <FilterPill
      displayValue={displayValue}
      filter={filter}
      hasValue={hasValue}
      onChange={onChange}
      onRemove={onRemove}
    >
      <DateTimeInput
        borderRadius="full"
        onChange={handleDateChange}
        placeholder={filter.config.placeholder}
        size="sm"
        value={typeof filter.value === "string" ? filter.value : ""}
        width="200px"
      />
    </FilterPill>
  );
};
