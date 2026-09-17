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
import { useParams } from "react-router-dom";

import { useDagVersionServiceGetDagVersionDiff } from "openapi/queries";

import { ProgressBar } from "src/system-components";

import { VersionCompareSelect } from "src/pages/Dag/Code/VersionCompareSelect";

import { ErrorAlert } from "src/components/ErrorAlert";

import { VersionDiff } from "./VersionDiff";

type SelectedVersionsProps = {
  readonly baseVersionNumber: number;
  readonly dagId: string;
  readonly targetVersionNumber: number;
};

/** Split out so the query only exists once both versions are chosen, rather than being disabled. */
const SelectedVersionsDiff = ({ baseVersionNumber, dagId, targetVersionNumber }: SelectedVersionsProps) => {
  const { data, error, isLoading } = useDagVersionServiceGetDagVersionDiff({
    baseVersionNumber,
    dagId,
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

export const Versions = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "" } = useParams();
  const [baseVersionNumber, setBaseVersionNumber] = useState<number | undefined>(undefined);
  const [targetVersionNumber, setTargetVersionNumber] = useState<number | undefined>(undefined);

  return (
    <Box p={2}>
      <Flex alignItems="flex-end" gap={4} mb={4}>
        <VersionCompareSelect
          label={translate("versions.base")}
          onVersionChange={setBaseVersionNumber}
          selectedVersionNumber={baseVersionNumber}
        />
        <VersionCompareSelect
          label={translate("versions.target")}
          onVersionChange={setTargetVersionNumber}
          selectedVersionNumber={targetVersionNumber}
        />
      </Flex>

      {baseVersionNumber === undefined || targetVersionNumber === undefined ? (
        <Text color="fg.muted">{translate("versions.selectPrompt")}</Text>
      ) : (
        <SelectedVersionsDiff
          baseVersionNumber={baseVersionNumber}
          dagId={dagId}
          targetVersionNumber={targetVersionNumber}
        />
      )}
    </Box>
  );
};
