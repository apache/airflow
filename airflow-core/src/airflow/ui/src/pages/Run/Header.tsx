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
import { HStack, Text, Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiBarChart } from "react-icons/fi";

import { useDeadlinesServiceGetDagDeadlineAlerts } from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import { HeaderCard } from "src/components/HeaderCard";
import { LimitedItemsList } from "src/components/LimitedItemsList";
import { MarkRunAsButton } from "src/components/MarkAs";
import { NeedsReviewButtonWithModal } from "src/components/NeedsReviewButton";
import NotePreview from "src/components/NotePreview";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import Time from "src/components/Time";
import { RouterLink } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import DeleteRunButton from "src/pages/DagRuns/DeleteRunButton";
import { useDagRunNote } from "src/queries/useDagRunNote";
import { getDuration } from "src/utils";

import { DeadlineStatus } from "./DeadlineStatus";

export const Header = ({ dagRun }: { readonly dagRun: DAGRunResponse }) => {
  const { t: translate } = useTranslation();
  const { isPending, note, onOpen, onSave, setNote } = useDagRunNote(dagRun);

  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;

  const { data: alertData } = useDeadlinesServiceGetDagDeadlineAlerts({ dagId });
  const hasDeadlineAlerts = (alertData?.total_entries ?? 0) > 0;

  return (
    <Box>
      <HeaderCard
        actions={
          <>
            <NeedsReviewButtonWithModal dagId={dagId} runId={dagRunId} />
            <ClearRunButton dagRun={dagRun} isHotkeyEnabled />
            <MarkRunAsButton dagRun={dagRun} isHotkeyEnabled />
            <DeleteRunButton dagRun={dagRun} />
          </>
        }
        icon={<FiBarChart />}
        state={dagRun.state}
        stats={[
          ...(dagRun.logical_date === null
            ? []
            : [
                {
                  label: translate("logicalDate"),
                  value: <Time datetime={dagRun.logical_date} />,
                },
              ]),
          ...(dagRun.partition_key === null
            ? []
            : [
                {
                  label: translate("dagRun.partitionKey"),
                  value: dagRun.partition_key,
                },
              ]),
          {
            label: translate("dagRun.runType"),
            value: (
              <HStack>
                <RunTypeIcon runType={dagRun.run_type} />
                <Text>{translate(`runTypes.${dagRun.run_type}`)}</Text>
              </HStack>
            ),
          },
          { label: translate("startDate"), value: <Time datetime={dagRun.start_date} /> },
          { label: translate("endDate"), value: <Time datetime={dagRun.end_date} /> },
          { label: translate("duration"), value: getDuration(dagRun.start_date, dagRun.end_date) },
          ...(dagRun.triggering_user_name === null
            ? []
            : [
                {
                  label: translate("dagRun.triggeringUser"),
                  value: (
                    <RouterLink
                      to={`/dag_runs?${SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN}=${encodeURIComponent(dagRun.triggering_user_name)}`}
                    >
                      <Text>{dagRun.triggering_user_name}</Text>
                    </RouterLink>
                  ),
                },
              ]),
          {
            label: translate("dagRun.dagVersions"),
            value: (
              <LimitedItemsList
                items={dagRun.dag_versions.map((version) => (
                  <DagVersion key={version.id} version={version} />
                ))}
                maxItems={4}
              />
            ),
          },
          ...(hasDeadlineAlerts
            ? [
                {
                  label: translate("dag:deadlineStatus.label"),
                  value: <DeadlineStatus dagId={dagId} dagRunId={dagRunId} endDate={dagRun.end_date} />,
                },
              ]
            : []),
        ]}
        title={dagRun.dag_run_id}
      />
      <NotePreview
        header={translate("note.dagRun")}
        isPending={isPending}
        note={note}
        onOpen={onOpen}
        onSave={onSave}
        setNote={setNote}
      />
    </Box>
  );
};
