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

import { Box, Flex, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";
import { useDebouncedCallback } from "use-debounce";

import { useDagVersionServiceGetDagVersionDiff } from "openapi/queries";

import { NumberInputField, NumberInputRoot, ProgressBar } from "src/system-components";

import { ErrorAlert } from "src/components/ErrorAlert";
import { VersionCompareSelect } from "src/components/VersionCompareSelect";

import { SearchParamsKeys } from "src/constants/searchParams";

import { VersionDiff } from "./VersionDiff";

// Mirrors MAX_ALLOWED_CHANGES on the diff endpoint, which is the real guard: this only spares
// the caller a rejected request, so a stale value costs a 422 the error alert explains.
const MAX_CHANGES_LIMIT = 5000;
// Every distinct bound is its own request, and each one re-reads and compares two whole serialized
// Dags, so a keystroke is too cheap a trigger.
const MAX_CHANGES_DEBOUNCE_MS = 400;

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
  const { data, error, isLoading } = useDagVersionServiceGetDagVersionDiff(
    {
      baseVersionNumber,
      dagId,
      maxChanges,
      targetVersionNumber,
    },
    undefined,
    // Changing the bound is a new query key; without this the table would blank out while it loads.
    { placeholderData: (previous) => previous },
  );

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
  // The field keeps what was typed while the debounced write settles; the URL only ever holds a
  // bound the endpoint accepts.
  const [maxChangesDraft, setMaxChangesDraft] = useState(maxChanges === undefined ? "" : `${maxChanges}`);

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

  const writeMaxChanges = useDebouncedCallback((value: string) => {
    const parsed = parsePositiveInt(value);

    updateParam(
      SearchParamsKeys.MAX_CHANGES,
      parsed === undefined ? "" : `${Math.min(parsed, MAX_CHANGES_LIMIT)}`,
    );
  }, MAX_CHANGES_DEBOUNCE_MS);

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
        <VStack alignItems="flex-start" gap={1}>
          <Text fontSize="xs">{translate("versions.maxChanges")}</Text>
          <NumberInputRoot
            max={MAX_CHANGES_LIMIT}
            min={1}
            onValueChange={({ value }) => {
              setMaxChangesDraft(value);
              writeMaxChanges(value);
            }}
            size="sm"
            value={maxChangesDraft}
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
