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
import { HStack, Text } from "@chakra-ui/react";
import { FiBarChart, FiMessageSquare } from "react-icons/fi";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { HeaderCard } from "src/components/HeaderCard";
import { LimitedItemsList } from "src/components/LimitedItemsList";
import { MarkRunAsButton } from "src/components/MarkAs";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import Time from "src/components/Time";
import { getDuration } from "src/utils";

export const Header = ({
  dagRun,
  isRefreshing,
}: {
  readonly dagRun: DAGRunResponse;
  readonly isRefreshing?: boolean;
}) => (
  <HeaderCard
    actions={
      <>
        {dagRun.note === null || dagRun.note.length === 0 ? undefined : (
          <DisplayMarkdownButton
            header="Dag Run Note"
            icon={<FiMessageSquare />}
            mdContent={dagRun.note}
            text="Note"
          />
        )}
        <ClearRunButton dagRun={dagRun} />
        <MarkRunAsButton dagRun={dagRun} />
      </>
    }
    icon={<FiBarChart />}
    isRefreshing={isRefreshing}
    state={dagRun.state}
    stats={[
      ...(dagRun.logical_date === null
        ? []
        : [
            {
              label: "Logical Date",
              value: <Time datetime={dagRun.logical_date} />,
            },
          ]),
      {
        label: "Run Type",
        value: (
          <HStack>
            <RunTypeIcon runType={dagRun.run_type} />
            <Text>{dagRun.run_type}</Text>
          </HStack>
        ),
      },
      { label: "Start", value: <Time datetime={dagRun.start_date} /> },
      { label: "End", value: <Time datetime={dagRun.end_date} /> },
      { label: "Duration", value: `${getDuration(dagRun.start_date, dagRun.end_date)}s` },
      {
        label: "Dag Version(s)",
        value: (
          <LimitedItemsList
            items={dagRun.dag_versions.map(({ version_number: versionNumber }) => `v${versionNumber}`)}
          />
        ),
      },
    ]}
    title={<Time datetime={dagRun.run_after} />}
  />
);
