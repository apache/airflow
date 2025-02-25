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
import { Text } from "@chakra-ui/react";
import { FiBookOpen, FiCalendar } from "react-icons/fi";
import { useParams } from "react-router-dom";

import type { DAGDetailsResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import DagRunInfo from "src/components/DagRunInfo";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { HeaderCard } from "src/components/HeaderCard";
import MenuButton from "src/components/Menu/MenuButton";
import ParseDag from "src/components/ParseDag";
import { TogglePause } from "src/components/TogglePause";
import { Tooltip } from "src/components/ui";

import { DagTags } from "../DagsList/DagTags";

export const Header = ({
  dag,
  dagWithRuns,
  isRefreshing,
}: {
  readonly dag?: DAGDetailsResponse;
  readonly dagWithRuns?: DAGWithLatestDagRunsResponse;
  readonly isRefreshing?: boolean;
}) => {
  // We would still like to show the dagId even if the dag object hasn't loaded yet
  const { dagId } = useParams();
  const latestRun = dagWithRuns?.latest_dag_runs ? dagWithRuns.latest_dag_runs[0] : undefined;

  const stats = [
    {
      label: "Schedule",
      value: Boolean(dag?.timetable_summary) ? (
        <Tooltip content={dag?.timetable_description}>
          <Text fontSize="sm">
            <FiCalendar style={{ display: "inline" }} /> {dag?.timetable_summary}
          </Text>
        </Tooltip>
      ) : undefined,
    },
    {
      label: "Latest Run",
      value:
        Boolean(latestRun) && latestRun !== undefined ? (
          <DagRunInfo
            endDate={latestRun.end_date}
            logicalDate={latestRun.logical_date}
            runAfter={latestRun.run_after}
            startDate={latestRun.start_date}
            state={latestRun.state}
          />
        ) : undefined,
    },
    {
      label: "Next Run",
      value: Boolean(dagWithRuns?.next_dagrun_run_after) ? (
        <DagRunInfo
          logicalDate={dagWithRuns?.next_dagrun_logical_date}
          runAfter={dagWithRuns?.next_dagrun_run_after as string}
        />
      ) : undefined,
    },
    {
      label: "Owner",
      value: dag?.owners.join(", "),
    },
    {
      label: "Tags",
      value: <DagTags tags={dag?.tags ?? []} />,
    },
  ];

  return (
    <HeaderCard
      actions={
        dag === undefined ? undefined : (
          <>
            {dag.doc_md === null ? undefined : (
              <DisplayMarkdownButton
                header="Dag Documentation"
                icon={<FiBookOpen />}
                mdContent={dag.doc_md}
                text="Dag Docs"
              />
            )}
            <ParseDag dagId={dag.dag_id} fileToken={dag.file_token} />
            <MenuButton dag={dag} />
          </>
        )
      }
      icon={<DagIcon />}
      isRefreshing={isRefreshing}
      stats={stats}
      subTitle={
        dag !== undefined && (
          <TogglePause dagDisplayName={dag.dag_display_name} dagId={dag.dag_id} isPaused={dag.is_paused} />
        )
      }
      title={dag?.dag_display_name ?? dagId}
    />
  );
};
