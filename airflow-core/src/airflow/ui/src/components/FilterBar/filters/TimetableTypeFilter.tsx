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
import { useState } from "react";

import { useTranslation } from "react-i18next";

import { useDagTimetableTypesInfinite } from "src/queries/useDagTimetableTypesInfinite";

import type { FilterPluginProps } from "../types";
import { MultiSelectPill } from "./MultiSelectPill";

export const TimetableTypeFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const { t: translate } = useTranslation("dags");
  const [pattern, setPattern] = useState("");

  const { data, fetchNextPage, fetchPreviousPage } = useDagTimetableTypesInfinite({
    limit: 10,
    timetableTypePrefixPattern: pattern,
  });

  return (
    <MultiSelectPill
      filter={filter}
      noOptionsMessage={translate("filters.noTimetableTypesFound")}
      onChange={onChange}
      onInputChange={setPattern}
      onMenuScrollToBottom={() => {
        void fetchNextPage();
      }}
      onMenuScrollToTop={() => {
        void fetchPreviousPage();
      }}
      onRemove={onRemove}
      options={(data?.pages.flatMap((page) => page.timetable_types) ?? []).map((type) => ({
        label: type,
        value: type,
      }))}
    />
  );
};
