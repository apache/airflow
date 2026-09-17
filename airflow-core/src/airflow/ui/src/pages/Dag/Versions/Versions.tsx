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
import { Box, Flex, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { useDagVersionServiceGetDagVersionDiff } from "openapi/queries";

import { NumberInputField, NumberInputRoot, ProgressBar } from "src/system-components";

import { ErrorAlert } from "src/components/ErrorAlert";
import { VersionCompareSelect } from "src/components/VersionCompareSelect";

import { SearchParamsKeys } from "src/constants/searchParams";

import { VersionDiff } from "./VersionDiff";

// Mirrors MAX_ALLOWED_CHANGES on the diff endpoint, which rejects anything larger.
const MAX_CHANGES_LIMIT = 5000;

type SelectedVersionsProps = {
  readonly baseVersionNumber: number;
  readonly dagId: string;
  readonly maxChanges: number | undefined;
  readonly targetVersionNumber: number;
};

/** Split out so the query only exists once both versions are chosen, rather than being disabled. */
const SelectedVersionsDiff = ({
  baseVersionNumber,
  dagId,
  maxChanges,
  targetVersionNumber,
}: SelectedVersionsProps) => {
  const { data, error, isLoading } = useDagVersionServiceGetDagVersionDiff({
    baseVersionNumber,
    dagId,
    maxChanges,
    targetVersionNumber,
  });

  return (
    <>
      <ErrorAlert error={error} />
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      {data === undefined ? undefined : (
        <VersionDiff
          baseVersionNumber={baseVersionNumber}
          diff={data}
          targetVersionNumber={targetVersionNumber}
        />
      )}
    </>
  );
};

const parsePositiveInt = (raw: string | null) => {
  const parsed = Number(raw);

  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
};

export const Versions = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const baseVersionNumber = parsePositiveInt(searchParams.get(SearchParamsKeys.BASE_VERSION_NUMBER));
  const targetVersionNumber = parsePositiveInt(searchParams.get(SearchParamsKeys.TARGET_VERSION_NUMBER));
  const requestedMaxChanges = parsePositiveInt(searchParams.get(SearchParamsKeys.MAX_CHANGES));
  // Clamped rather than passed through, so a hand-edited link cannot make the endpoint reject the
  // request. The input below shows the clamped value, never a limit that is not in effect.
  const maxChanges =
    requestedMaxChanges === undefined ? undefined : Math.min(requestedMaxChanges, MAX_CHANGES_LIMIT);

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

  return (
    <Box p={2}>
      <Flex alignItems="flex-end" gap={4} mb={4}>
        <VersionCompareSelect
          label={translate("versions.base")}
          onVersionChange={(versionNumber) =>
            updateParam(SearchParamsKeys.BASE_VERSION_NUMBER, versionNumber.toString())
          }
          selectedVersionNumber={baseVersionNumber}
        />
        <VersionCompareSelect
          label={translate("versions.target")}
          onVersionChange={(versionNumber) =>
            updateParam(SearchParamsKeys.TARGET_VERSION_NUMBER, versionNumber.toString())
          }
          selectedVersionNumber={targetVersionNumber}
        />
        <VStack alignItems="flex-start" gap={1}>
          <Text fontSize="xs">{translate("versions.maxChanges")}</Text>
          <NumberInputRoot
            max={MAX_CHANGES_LIMIT}
            min={1}
            onValueChange={({ value }) => updateParam(SearchParamsKeys.MAX_CHANGES, value)}
            size="sm"
            value={maxChanges === undefined ? "" : maxChanges.toString()}
            w={28}
          >
            <NumberInputField
              aria-label={translate("versions.maxChanges")}
              placeholder={translate("versions.maxChangesDefault")}
            />
          </NumberInputRoot>
        </VStack>
      </Flex>

      {baseVersionNumber === undefined || targetVersionNumber === undefined ? (
        <Text color="fg.muted">{translate("versions.selectPrompt")}</Text>
      ) : (
        <SelectedVersionsDiff
          baseVersionNumber={baseVersionNumber}
          dagId={dagId}
          maxChanges={maxChanges}
          targetVersionNumber={targetVersionNumber}
        />
      )}
    </Box>
  );
};
