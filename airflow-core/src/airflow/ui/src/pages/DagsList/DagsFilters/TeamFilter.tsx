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
import { Select as ReactSelect, type MultiValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";

import { useTeamsServiceListTeams } from "openapi/queries";

type Props = {
  readonly onChange: (teams: Array<string>) => void;
  readonly selectedTeams: Array<string>;
};

export const TeamFilter = ({ onChange, selectedTeams }: Props) => {
  const { t: translate } = useTranslation("common");
  const { data } = useTeamsServiceListTeams({ orderBy: ["name"] });

  const options = (data?.teams ?? []).map((team) => ({
    label: team.name,
    value: team.name,
  }));

  const handleChange = (selected: MultiValue<{ label: string; value: string }>) => {
    onChange(selected.map(({ value }) => value));
  };

  return (
    <Box>
      <Field.Root>
        <ReactSelect
          aria-label={translate("dagDetails.team")}
          chakraStyles={{
            clearIndicator: (provided) => ({
              ...provided,
              color: "gray.fg",
            }),
            container: (provided) => ({
              ...provided,
              maxWidth: 200,
              minWidth: 64,
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
          isMulti
          noOptionsMessage={() => translate("table.noTeamsFound")}
          onChange={handleChange}
          options={options}
          placeholder={translate("dagDetails.team")}
          value={selectedTeams.map((team) => ({
            label: team,
            value: team,
          }))}
        />
      </Field.Root>
    </Box>
  );
};
