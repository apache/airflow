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
import type { FilterPluginProps } from "../types";
import { MultiSelectFilter } from "./MultiSelectFilter";

export const WeekdayFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const options = (filter.config.options ?? []).map((option) => ({
    label: typeof option.label === "string" ? option.label : option.value,
    value: option.value,
  }));
  const values = Array.isArray(filter.value) ? filter.value : [];
  const labels = values.map((value) => options.find((option) => option.value === value)?.label ?? value);

  return (
    <MultiSelectFilter
      filter={{
        ...filter,
        config: {
          ...filter.config,
          options: options.map((option) => ({ label: option.label, value: option.label })),
        },
        value: labels,
      }}
      onChange={(value) =>
        onChange(
          Array.isArray(value)
            ? options.filter((option) => value.includes(option.label)).map((option) => option.value)
            : [],
        )
      }
      onRemove={onRemove}
    />
  );
};
