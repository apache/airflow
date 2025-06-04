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
import { Button } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuX } from "react-icons/lu";

type Props = {
  readonly filterCount: number;
  readonly onClearFilters: () => void;
};

export const ResetButton = ({ filterCount, onClearFilters }: Props) => {
  const { t: translate } = useTranslation("common");

  if (filterCount === 0) {
    return undefined;
  }

  return (
    <Button onClick={onClearFilters} size="sm" variant="outline">
      <LuX /> {translate("table.filters.reset")}{" "}
      {filterCount === 1 ? translate("table.filters.filter_one") : translate("table.filters.filter_other")}
    </Button>
  );
};
