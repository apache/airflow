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
import { CreatableSelect, type MultiValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";

type Props = {
  readonly onChange: (owners: Array<string>) => void;
  readonly values: Array<string>;
};

type OwnerOption = { label: string; value: string };

export const OwnerFilter = ({ onChange, values }: Props) => {
  const { t: translate } = useTranslation("common");
  const options = values.map((owner) => ({ label: owner, value: owner }));

  return (
    <Box flex="0 1 200px" maxWidth="100%" width="200px">
      <Field.Root>
        <CreatableSelect<OwnerOption, true>
          aria-label={translate("owner")}
          isClearable
          isMulti
          onChange={(selected: MultiValue<OwnerOption>) => onChange(selected.map(({ value }) => value))}
          options={options}
          placeholder={translate("owner")}
          value={options}
        />
      </Field.Root>
    </Box>
  );
};
