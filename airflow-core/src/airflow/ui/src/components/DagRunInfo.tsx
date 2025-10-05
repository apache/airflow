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
import { VStack, Text, Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";
import { getDuration } from "src/utils";

type Props = {
  readonly endDate?: string | null;
  readonly logicalDate?: string | null;
  readonly runAfter: string;
  readonly startDate?: string | null;
  readonly state?: DAGRunResponse["state"];
};

const DagRunInfo = ({ endDate, logicalDate, runAfter, startDate, state }: Props) => {
  const { t: translate } = useTranslation("common");

  return (
    <Tooltip
      content={
        <VStack align="left" gap={0}>
          {state === undefined ? undefined : (
            <Text>
              {translate("state")}: {state}
            </Text>
          )}
          {Boolean(logicalDate) ? (
            <Text>
              {translate("logicalDate")}: <Time datetime={logicalDate} />
            </Text>
          ) : undefined}
          {Boolean(startDate) ? (
            <Text>
              {translate("startDate")}: <Time datetime={startDate} />
            </Text>
          ) : undefined}
          {Boolean(endDate) ? (
            <Text>
              {translate("endDate")}: <Time datetime={endDate} />
            </Text>
          ) : undefined}
          {Boolean(startDate) ? (
            <Text>
              {translate("duration")}: {getDuration(startDate, endDate)}
            </Text>
          ) : undefined}
        </VStack>
      }
    >
      <Box>
        <Time datetime={runAfter} mr={2} showTooltip={false} />
        {state === undefined ? undefined : (
          <StateBadge aria-label={state} data-testid="state-badge" state={state} />
        )}
      </Box>
    </Tooltip>
  );
};

export default DagRunInfo;
