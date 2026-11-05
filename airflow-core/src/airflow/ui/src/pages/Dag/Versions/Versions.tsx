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

import { Box, Flex, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { VersionCompareSelect } from "src/components/VersionCompareSelect";

import { SearchParamsKeys } from "src/constants/searchParams";

import { MAX_CHANGES_LIMIT, MaxChangesField } from "./MaxChangesField";
import { SelectedVersionsDiff } from "./SelectedVersionsDiff";

const parsePositiveInt = (raw: string | null) => {
  const parsed = Number(raw);

  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : undefined;
};

export const Versions = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const baseVersionNumber = parsePositiveInt(searchParams.get(SearchParamsKeys.BASE_VERSION_NUMBER));
  const targetVersionNumber = parsePositiveInt(searchParams.get(SearchParamsKeys.TARGET_VERSION_NUMBER));
  const requestedMaxChanges = parsePositiveInt(searchParams.get(SearchParamsKeys.MAX_CHANGES));
  // Clamped rather than passed through, so a hand-edited link cannot make the endpoint reject the
  // request. An empty bound means the endpoint's own default applies.
  const maxChanges =
    requestedMaxChanges === undefined ? undefined : Math.min(requestedMaxChanges, MAX_CHANGES_LIMIT);
  // Only the comparison knows whether its bound cut it short, and only the field can lift it.
  const [truncated, setTruncated] = useState(false);

  const updateParam = (key: string, value: string) =>
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);

        if (value === "") {
          next.delete(key);
        } else {
          next.set(key, value);
        }

        return next;
      },
      { replace: true },
    );

  const writeMaxChanges = (value: string) => {
    const parsed = parsePositiveInt(value);

    updateParam(
      SearchParamsKeys.MAX_CHANGES,
      parsed === undefined ? "" : `${Math.min(parsed, MAX_CHANGES_LIMIT)}`,
    );
  };

  return (
    <Box p={2}>
      <Flex alignItems="flex-end" gap={4} mb={4}>
        <VersionCompareSelect
          label={translate("versions.base")}
          onVersionChange={(versionNumber) =>
            updateParam(SearchParamsKeys.BASE_VERSION_NUMBER, `${versionNumber}`)
          }
          selectedVersionNumber={baseVersionNumber}
        />
        <VersionCompareSelect
          label={translate("versions.target")}
          onVersionChange={(versionNumber) =>
            updateParam(SearchParamsKeys.TARGET_VERSION_NUMBER, `${versionNumber}`)
          }
          selectedVersionNumber={targetVersionNumber}
        />
        <MaxChangesField maxChanges={maxChanges} onCommit={writeMaxChanges} truncated={truncated} />
      </Flex>

      {baseVersionNumber === undefined || targetVersionNumber === undefined ? (
        <Text color="fg.muted">{translate("versions.selectPrompt")}</Text>
      ) : (
        <SelectedVersionsDiff
          baseVersionNumber={baseVersionNumber}
          dagId={dagId}
          maxChanges={maxChanges}
          onTruncatedChange={setTruncated}
          targetVersionNumber={targetVersionNumber}
        />
      )}
    </Box>
  );
};
