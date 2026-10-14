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
import type { ReactNode } from "react";

import { useTranslation } from "react-i18next";
import { AiOutlineGroup } from "react-icons/ai";
import { FiBookOpen } from "react-icons/fi";
import { useParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import { ClearTaskInstanceButton } from "src/components/Clear";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { HeaderCard } from "src/components/HeaderCard";
import { MarkTaskGroupAsButton } from "src/components/MarkAs";
import Time from "src/components/Time";

import { useLoopGroupNode } from "src/queries/useIsLoopGroup";
import { formatNumber, useDurationFormat } from "src/utils";

import { useLoopOutcomeStats } from "./useLoopOutcomeStats";

export const Header = ({ taskInstance }: { readonly taskInstance: LightGridTaskInstanceSummary }) => {
  const { i18n, t: translate } = useTranslation();
  const { formatElapsed } = useDurationFormat();
  const { dagId = "", groupId = "", runId = "" } = useParams();
  const outcomeStats = useLoopOutcomeStats({ dagId, groupId, runId });
  const groupNode = useLoopGroupNode(groupId);
  const docMd = groupNode?.doc_md;
  const entries: Array<{ label: string; value: number | ReactNode | string }> = [];

  Object.entries(taskInstance.child_states ?? {}).forEach(([state, count]) => {
    entries.push({
      label: translate("total", { state: translate(`states.${state.toLowerCase()}`) }),
      value: formatNumber(count, i18n.language),
    });
  });
  const stats = [
    ...outcomeStats,
    ...entries,
    { label: translate("startDate"), value: <Time datetime={taskInstance.min_start_date} /> },
    { label: translate("endDate"), value: <Time datetime={taskInstance.max_end_date} /> },
    ...(Boolean(taskInstance.max_end_date)
      ? [
          {
            label: translate("duration"),
            value: formatElapsed(taskInstance.min_start_date, taskInstance.max_end_date),
          },
        ]
      : []),
  ];

  return (
    <HeaderCard
      actions={
        <>
          {docMd === null || docMd === undefined ? undefined : (
            <DisplayMarkdownButton
              header={translate("taskGroup.documentation")}
              icon={<FiBookOpen />}
              mdContent={docMd}
              text={translate("docs.documentation")}
            />
          )}
          <ClearTaskInstanceButton
            bg="bg"
            groupTaskInstance={taskInstance}
            isHotkeyEnabled
            variant="outline"
          />
          <MarkTaskGroupAsButton bg="bg" groupTaskInstance={taskInstance} isHotkeyEnabled variant="outline" />
        </>
      }
      icon={<AiOutlineGroup />}
      state={taskInstance.state}
      stats={stats}
      subTitle={<Time datetime={taskInstance.min_start_date} />}
      title={taskInstance.task_display_name}
      type="taskGroup"
    />
  );
};
