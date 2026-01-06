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

import { HStack, Spinner } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import type { TaskInstanceState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { Stat } from "src/components/Stat";

type Props = {
  readonly dagId: string;
  readonly runId: string;
};

export const TaskInstanceSummary = ({ dagId, runId }: Props) => {
  const { t: translate } = useTranslation("common");

  const { data, isLoading } = useTaskInstanceServiceGetTaskInstances({
    dagId,
    dagRunId: runId,
    limit: 1000,
  });

  if (isLoading) {
    return (
      <Stat label={translate("taskInstanceSummary")}>
        <Spinner size="sm" />
      </Stat>
    );
  }

  if (!data) {
    return null;
  }

  // Count task instances by state
  const stateCounts: Record<string, number> = {};

  data.task_instances.forEach((ti) => {
    const state = ti.state ?? "no_status";
    stateCounts[state] = (stateCounts[state] ?? 0) + 1;
  });

  const stateEntries = Object.entries(stateCounts);

  if (stateEntries.length === 0) {
    return null;
  }

  return (
    <Stat data-testid="task-instance-summary" label={translate("taskInstanceSummary")}>
      <HStack flexWrap="wrap" gap={1}>
        {stateEntries.map(([state, count]) => (
          <StateBadge key={state} state={state as TaskInstanceState}>
            {count}
          </StateBadge>
        ))}
      </HStack>
    </Stat>
  );
};
