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
import { Button, ButtonGroup } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuUserRoundPen } from "react-icons/lu";

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
    <ButtonGroup attached size="sm" variant="outline">
      <Button
        bg={isAll ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        onClick={onStateChange}
        value="all"
        variant={isAll ? "solid" : "outline"}
      >
        {translate("dags:filters.paused.all")}
      </Button>
      <Button
        bg={isFailed ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        data-testid="dags-failed-filter"
        onClick={onStateChange}
        value="failed"
        variant={isFailed ? "solid" : "outline"}
      >
        <StateBadge state="failed" />
        {translate("common:states.failed")}
      </Button>
      <Button
        bg={isQueued ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        data-testid="dags-queued-filter"
        onClick={onStateChange}
        value="queued"
        variant={isQueued ? "solid" : "outline"}
      >
        <StateBadge state="queued" />
        {translate("common:states.queued")}
      </Button>
      <Button
        bg={isRunning ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        data-testid="dags-running-filter"
        onClick={onStateChange}
        value="running"
        variant={isRunning ? "solid" : "outline"}
      >
        <StateBadge state="running" />
        {translate("common:states.running")}
      </Button>
      <Button
        bg={isSuccess ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        data-testid="dags-success-filter"
        onClick={onStateChange}
        value="success"
        variant={isSuccess ? "solid" : "outline"}
      >
        <StateBadge state="success" />
        {translate("common:states.success")}
      </Button>
      <Button
        bg={needsReview ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        data-testid="dags-needs-review-filter"
        onClick={onStateChange}
        value="needs_review"
        variant={needsReview ? "solid" : "outline"}
      >
        <StateBadge colorPalette="deferred">
          <LuUserRoundPen />
        </StateBadge>
        {translate("hitl:requiredAction_other")}
      </Button>
    </ButtonGroup>
  );
};
