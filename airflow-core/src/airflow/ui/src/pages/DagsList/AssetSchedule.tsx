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
import dayjs from "dayjs";
import { FiDatabase } from "react-icons/fi";

import { useAssetServiceNextRunAssets } from "openapi/queries";
import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { AssetExpression, type ExpressionType } from "src/components/AssetExpression";
import type { NextRunEvent } from "src/components/AssetExpression/types";
import { Button, Popover } from "src/components/ui";

type Props = {
  readonly dag: DAGWithLatestDagRunsResponse;
};

export const AssetSchedule = ({ dag }: Props) => {
  const { data: nextRun, isLoading } = useAssetServiceNextRunAssets({ dagId: dag.dag_id });

  const nextRunEvents =
    nextRun !== undefined && "events" in nextRun ? (nextRun.events as Array<NextRunEvent>) : [];

  const pendingEvents = nextRunEvents.filter((ev) => {
    if (ev.lastUpdate !== null && dag.latest_dag_runs[0]?.run_after === undefined) {
      return true;
    }
    if (ev.lastUpdate !== null && dag.latest_dag_runs[0]?.run_after !== undefined) {
      return dayjs(ev.lastUpdate).isAfter(dag.latest_dag_runs[0]?.run_after);
    }

    return false;
  });

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
      <Popover.Trigger asChild>
        <Button loading={isLoading} paddingInline={0} size="sm" variant="ghost">
          <FiDatabase style={{ display: "inline" }} />
          {nextRunEvents.length === 1 ? (
            nextRunEvents[0]?.uri
          ) : (
            <>
              {pendingEvents.length} of {nextRunEvents.length} datasets updated
            </>
          )}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          <AssetExpression events={pendingEvents} expression={dag.asset_expression as ExpressionType} />
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
