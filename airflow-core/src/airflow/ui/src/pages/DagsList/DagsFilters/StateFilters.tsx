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
import { HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuUserRoundPen } from "react-icons/lu";

import { QuickFilterButton } from "src/components/QuickFilterButton";
import { StateBadge } from "src/components/StateBadge";

type Props = {
  readonly isAll: boolean;
  readonly isFailed: boolean;
  readonly isQueued: boolean;
  readonly isRunning: boolean;
  readonly isSuccess: boolean;
  readonly needsReview: boolean;
  readonly onStateChange: React.MouseEventHandler<HTMLButtonElement>;
};

export const StateFilters = ({
  isAll,
  isFailed,
  isQueued,
  isRunning,
  isSuccess,
  needsReview,
  onStateChange,
}: Props) => {
  const { t: translate } = useTranslation(["dags", "common", "hitl"]);

  return (
    <HStack>
      <QuickFilterButton isActive={isAll} onClick={onStateChange} value="all">
        {translate("dags:filters.paused.all")}
      </QuickFilterButton>
      <QuickFilterButton
        data-testid="dags-failed-filter"
        isActive={isFailed}
        onClick={onStateChange}
        value="failed"
      >
        <StateBadge state="failed" />
        {translate("common:states.failed")}
      </QuickFilterButton>
      <QuickFilterButton
        data-testid="dags-queued-filter"
        isActive={isQueued}
        onClick={onStateChange}
        value="queued"
      >
        <StateBadge state="queued" />
        {translate("common:states.queued")}
      </QuickFilterButton>
      <QuickFilterButton
        data-testid="dags-running-filter"
        isActive={isRunning}
        onClick={onStateChange}
        value="running"
      >
        <StateBadge state="running" />
        {translate("common:states.running")}
      </QuickFilterButton>
      <QuickFilterButton
        data-testid="dags-success-filter"
        isActive={isSuccess}
        onClick={onStateChange}
        value="success"
      >
        <StateBadge state="success" />
        {translate("common:states.success")}
      </QuickFilterButton>
      <QuickFilterButton
        data-testid="dags-needs-review-filter"
        isActive={needsReview}
        onClick={onStateChange}
        value="needs_review"
      >
        <StateBadge colorPalette="deferred">
          <LuUserRoundPen />
        </StateBadge>
        {translate("hitl:requiredAction_other")}
      </QuickFilterButton>
    </HStack>
  );
};
