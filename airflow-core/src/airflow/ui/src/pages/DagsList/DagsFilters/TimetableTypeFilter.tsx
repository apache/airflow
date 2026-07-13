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
import { useTranslation } from "react-i18next";
import { FiClock } from "react-icons/fi";

import { SearchBar } from "src/components/SearchBar";

type Props = {
  readonly onChange: (value: string) => void;
  readonly value: string;
};

export const TimetableTypeFilter = ({ onChange, value: initialValue }: Props) => {
  const { t: translate } = useTranslation("dags");

  return (
    <SearchBar
      clearButtonAriaLabel={translate("filters.timetableTypeClear")}
      clearButtonTestId="clear-timetable-type-filter"
      defaultValue={initialValue}
      hotkeyDisabled
      inputAriaLabel={translate("filters.timetableType")}
      inputPaddingRight={0}
      inputTestId="dags-timetable-type-filter"
      maxWidth="260px"
      minWidth="220px"
      onChange={(newValue) => onChange(newValue.trim())}
      placeholder={translate("filters.timetableType")}
      startElement={<FiClock />}
    />
  );
};
