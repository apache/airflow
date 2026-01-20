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

import type { LatestRunStats, TaskInstanceState } from "openapi/requests/types.gen";
import { Stat } from "src/components/Stat";
import { StateBadge } from "src/components/StateBadge";

type Props = {
  readonly latestRunStats: LatestRunStats | null | undefined;
};

export const TaskInstanceSummary = ({ latestRunStats }: Props) => {
  const { t: translate } = useTranslation("common");

  if (!latestRunStats) {
    return null;
  }

  const stateEntries = Object.entries(latestRunStats.task_instance_counts);

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
