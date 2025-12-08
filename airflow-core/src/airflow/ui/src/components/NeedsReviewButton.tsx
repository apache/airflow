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
import { LuUserRoundPen } from "react-icons/lu";

import { useTaskInstanceServiceGetHitlDetails } from "openapi/queries";
import { useAutoRefresh } from "src/utils/query";

import { StatsCard } from "./StatsCard";

export const NeedsReviewButton = ({
  dagId,
  runId,
  taskId,
}: {
  readonly dagId?: string;
  readonly runId?: string;
  readonly taskId?: string;
}) => {
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true, dagId });

  const { data: hitlStatsData, isLoading } = useTaskInstanceServiceGetHitlDetails(
    {
      dagId: dagId ?? "~",
      dagRunId: runId ?? "~",
      responseReceived: false,
      state: ["deferred"],
      taskId,
    },
    undefined,
    {
      refetchInterval,
    },
  );

  const hitlTIsCount = hitlStatsData?.hitl_details.length ?? 0;
  const { i18n, t: translate } = useTranslation("hitl");

  const isRTL = i18n.dir() === "rtl";

  return hitlTIsCount > 0 ? (
    <Box maxW="250px">
      <StatsCard
        colorScheme="deferred"
        count={hitlTIsCount}
        icon={<LuUserRoundPen />}
        isLoading={isLoading}
        isRTL={isRTL}
        label={translate("requiredAction_other")}
        link="required_actions?response_received=false"
      />
    </Box>
  ) : undefined;
};
