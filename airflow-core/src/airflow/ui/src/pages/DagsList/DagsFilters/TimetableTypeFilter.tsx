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
import { Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { DagsFilterSelect } from "./DagsFilterSelect";

type Props = {
  readonly onChange: (timetableTypes: Array<string>) => void;
  readonly onInputChange: (value: string) => void;
  readonly onMenuScrollToBottom: () => void;
  readonly onMenuScrollToTop: () => void;
  readonly timetableTypes: Array<string>;
  readonly values: Array<string>;
};

export const TimetableTypeFilter = ({
  onChange,
  onInputChange,
  onMenuScrollToBottom,
  onMenuScrollToTop,
  timetableTypes,
  values,
}: Props) => {
  const { t: translate } = useTranslation("dags");

  return (
    <Box flex="0 1 300px" maxWidth="100%" width="300px">
      <DagsFilterSelect
        ariaLabel={translate("filters.timetableType")}
        noOptionsMessage={translate("filters.noTimetableTypesFound")}
        onChange={onChange}
        onInputChange={onInputChange}
        onMenuScrollToBottom={onMenuScrollToBottom}
        onMenuScrollToTop={onMenuScrollToTop}
        options={timetableTypes}
        placeholder={translate("filters.timetableType")}
        values={values}
      />
    </Box>
  );
};
