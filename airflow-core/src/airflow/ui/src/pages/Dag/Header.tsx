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
import { Link } from "@chakra-ui/react";
import { Button, Menu, Portal } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiBookOpen } from "react-icons/fi";
import { LuMenu } from "react-icons/lu";
import { useParams, Link as RouterLink } from "react-router-dom";

import type { DAGDetailsResponse, DagRunState } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import DeleteDagButton from "src/components/DagActions/DeleteDagButton";
import { FavoriteDagButton } from "src/components/DagActions/FavoriteDagButton";
import ParseDag from "src/components/DagActions/ParseDag";
import DagRunInfo from "src/components/DagRunInfo";
import { DagVersion } from "src/components/DagVersion";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { HeaderCard } from "src/components/HeaderCard";
import { TogglePause } from "src/components/TogglePause";

import { DagOwners } from "../DagsList/DagOwners";
import { DagTags } from "../DagsList/DagTags";
import { Schedule } from "../DagsList/Schedule";

type LatestRunInfo = {
  dag_id: string;
  end_date: string | null;
  logical_date: string | null;
  run_after: string;
  run_id: string;
  start_date: string | null;
  state: DagRunState;
};

export const Header = ({
  dag,
  latestRunInfo,
}: {
  readonly dag?: DAGDetailsResponse;
  readonly latestRunInfo?: LatestRunInfo;
}) => {
  const { t: translate } = useTranslation(["common", "dag"]);
  // We would still like to show the dagId even if the dag object hasn't loaded yet
  const { dagId } = useParams();

  const stats = [
    {
      label: translate("dagDetails.schedule"),
      value:
        dag === undefined ? undefined : (
          <Schedule
            assetExpression={dag.asset_expression}
            dagId={dag.dag_id}
            latestRunAfter={latestRunInfo?.run_after}
            timetableDescription={dag.timetable_description}
            timetableSummary={dag.timetable_summary}
          />
        ),
    },
    {
      label: translate("dagDetails.latestRun"),
      value:
        Boolean(latestRunInfo) && latestRunInfo !== undefined ? (
          <Link asChild color="fg.info">
            <RouterLink to={`/dags/${latestRunInfo.dag_id}/runs/${latestRunInfo.run_id}`}>
              <DagRunInfo
                endDate={latestRunInfo.end_date}
                logicalDate={latestRunInfo.logical_date}
                runAfter={latestRunInfo.run_after}
                startDate={latestRunInfo.start_date}
                state={latestRunInfo.state}
              />
            </RouterLink>
          </Link>
        ) : undefined,
    },
    {
      label: translate("dagDetails.nextRun"),
      value: Boolean(dag?.next_dagrun_run_after) ? (
        <DagRunInfo
          logicalDate={dag?.next_dagrun_logical_date}
          runAfter={dag?.next_dagrun_run_after as string}
        />
      ) : undefined,
    },
    {
      label: translate("dagDetails.owner"),
      value: <DagOwners ownerLinks={dag?.owner_links ?? undefined} owners={dag?.owners} />,
    },
    {
      label: translate("dagDetails.tags"),
      value: <DagTags tags={dag?.tags ?? []} />,
    },
    {
      label: translate("dagDetails.latestDagVersion"),
      value: <DagVersion version={dag?.latest_dag_version} />,
    },
  ];

  return (
    <HeaderCard
      actions={
        dag === undefined ? undefined : (
          <>
            {dag.doc_md === null ? undefined : (
              <DisplayMarkdownButton
                header={translate("dagDetails.documentation")}
                icon={<FiBookOpen />}
                mdContent={dag.doc_md}
                text={translate("dag:header.buttons.dagDocs")}
              />
            )}
            <FavoriteDagButton dagId={dag.dag_id} isFavorite={dag.is_favorite} withText />
            <Menu.Root>
              <Menu.Trigger asChild>
                <Button aria-label={translate("dag:header.buttons.advanced")} variant="outline">
                  <LuMenu />
                </Button>
              </Menu.Trigger>
              <Portal>
                <Menu.Positioner>
                  <Menu.Content>
                    <Menu.Item value="reparse">
                      <ParseDag dagId={dag.dag_id} fileToken={dag.file_token} width="100%" />
                    </Menu.Item>
                    <Menu.Item closeOnSelect={false} value="delete">
                      <DeleteDagButton
                        dagDisplayName={dag.dag_display_name}
                        dagId={dag.dag_id}
                        width="100%"
                      />
                    </Menu.Item>
                  </Menu.Content>
                </Menu.Positioner>
              </Portal>
            </Menu.Root>
          </>
        )
      }
      icon={<DagIcon />}
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
