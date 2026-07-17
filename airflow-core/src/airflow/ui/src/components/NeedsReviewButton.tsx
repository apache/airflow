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
import { Box, Button, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuUserRoundPen } from "react-icons/lu";
import { Link } from "react-router-dom";

import { useTaskInstanceServiceGetHitlDetails } from "openapi/queries";
import { HITLReviewModal } from "src/components/HITLReview/HITLReviewModal.tsx";
import { useHITLReviewModalRouteSync } from "src/components/HITLReview/useHITLReviewModalRouteSync.ts";
import { useAutoRefresh } from "src/utils/query";

import { StatsCard } from "./StatsCard";

const usePendingHitl = ({
  dagId,
  runId,
  taskId,
}: {
  readonly dagId?: string;
  readonly runId?: string;
  readonly taskId?: string;
}) => {
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true, dagId });

  const {
    data: pendingHitlData,
    isError,
    isLoading,
  } = useTaskInstanceServiceGetHitlDetails(
    {
      dagId: dagId ?? "~",
      dagRunId: runId ?? "~",
      orderBy: ["dag_id", "run_after", "created_at", "task_display_name"],
      responseReceived: false,
      state: ["deferred", "awaiting_input"],
      taskId,
    },
    undefined,
    {
      refetchInterval,
    },
  );

  return {
    isError,
    isLoading,
    pendingHitlData,
  };
};

const useCompletedHitl = ({
  dagId,
  enabled,
  runId,
}: {
  readonly dagId?: string;
  readonly enabled: boolean;
  readonly runId?: string;
}) => {
  const {
    data: completedHitlData,
    isError,
    isLoading,
  } = useTaskInstanceServiceGetHitlDetails(
    {
      dagId: dagId ?? "~",
      dagRunId: runId ?? "~",
      orderBy: ["dag_id", "run_after", "created_at", "task_display_name"],
      responseReceived: true,
    },
    undefined,
    {
      enabled,
    },
  );

  return { completedHitlData, isError, isLoading };
};

const NeedsReviewButtonCard = ({
  hitlTIsCount,
  isLoading,
  link,
  onClick,
}: {
  readonly hitlTIsCount: number;
  readonly isLoading: boolean;
  readonly link?: string;
  readonly onClick?: () => void;
}) => {
  const { i18n, t: translate } = useTranslation("hitl");

  const isRTL = i18n.dir() === "rtl";

  return hitlTIsCount > 0 ? (
    <Box maxW="250px">
      <StatsCard
        colorScheme="awaiting_input"
        count={hitlTIsCount}
        icon={<LuUserRoundPen />}
        isLoading={isLoading}
        isRTL={isRTL}
        label={translate("requiredAction_other")}
        link={link}
        onClick={onClick}
      />
    </Box>
  ) : undefined;
};

export const NeedsReviewButton = ({
  dagId,
  runId,
  taskId,
}: {
  readonly dagId?: string;
  readonly runId?: string;
  readonly taskId?: string;
}) => {
  const { isLoading, pendingHitlData } = usePendingHitl({ dagId, runId, taskId });
  const hitlTIsCount = pendingHitlData?.total_entries ?? 0;

  return (
    <NeedsReviewButtonCard
      hitlTIsCount={hitlTIsCount}
      isLoading={isLoading}
      link="required_actions?response_received=false"
    />
  );
};

export const NeedsReviewButtonWithModal = ({
  dagId,
  runId,
}: {
  readonly dagId?: string;
  readonly runId?: string;
}) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { onCloseHITLReview } = useHITLReviewModalRouteSync({
    onClose,
    onOpen,
  });
  const { isError: pendingHitlIsError, isLoading, pendingHitlData } = usePendingHitl({ dagId, runId });
  const {
    completedHitlData,
    isError: completedHitlIsError,
    isLoading: isLoadingCompletedHitl,
  } = useCompletedHitl({
    dagId,
    enabled: open,
    runId,
  });
  const { t: translate } = useTranslation("hitl");
  const hitlTIsCount = pendingHitlData?.total_entries ?? 0;

  return (
    <>
      <NeedsReviewButtonCard hitlTIsCount={hitlTIsCount} isLoading={isLoading} onClick={onOpen} />
      <HITLReviewModal
        completedHitl={{
          data: completedHitlData?.hitl_details ?? [],
          isError: completedHitlIsError,
          isLoading: isLoadingCompletedHitl,
          total: completedHitlData?.total_entries,
        }}
        headerAction={
          <Button asChild size="sm" variant="outline">
            <Link onClick={onCloseHITLReview} to="/required_actions?response_received=false">
              {translate("review.viewAll")}
            </Link>
          </Button>
        }
        onClose={onCloseHITLReview}
        open={open}
        pendingHitl={{
          data: pendingHitlData?.hitl_details ?? [],
          isError: pendingHitlIsError,
          isLoading,
          total: pendingHitlData?.total_entries,
        }}
      />
    </>
  );
};
