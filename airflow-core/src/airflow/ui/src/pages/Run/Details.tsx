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
import { Box, Flex, HStack, StackSeparator, Table, Text, VStack } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import { useDagRunServiceGetDagRun } from "openapi/queries";
import { DagVersionDetails } from "src/components/DagVersionDetails";
import RenderedJsonField from "src/components/RenderedJsonField";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";
import { getDuration, isStatePending, useAutoRefresh } from "src/utils";

export const Details = () => {
  const { dagId = "", runId = "" } = useParams();

  const refetchInterval = useAutoRefresh({ dagId });

  const { data: dagRun } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId,
    },
    undefined,
    { refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false) },
  );

  // TODO : Render DagRun configuration object
  return (
    <Box p={2}>
      {dagRun === undefined ? (
        <div />
      ) : (
        <Table.Root striped>
          <Table.Body>
            <Table.Row>
              <Table.Cell>State</Table.Cell>
              <Table.Cell>
                <Flex gap={1}>
                  <StateBadge state={dagRun.state} />
                  {dagRun.state}
                </Flex>
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Run ID</Table.Cell>
              <Table.Cell>
                <HStack>
                  {dagRun.dag_run_id}
                  <ClipboardRoot value={dagRun.dag_run_id}>
                    <ClipboardIconButton />
                  </ClipboardRoot>
                </HStack>
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Run Type</Table.Cell>
              <Table.Cell>
                <HStack>
                  <RunTypeIcon runType={dagRun.run_type} />
                  <Text>{dagRun.run_type}</Text>
                </HStack>
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Run Duration</Table.Cell>
              <Table.Cell>{getDuration(dagRun.start_date, dagRun.end_date)}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Last Scheduling Decision</Table.Cell>
              <Table.Cell>
                <Time datetime={dagRun.last_scheduling_decision} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Queued at</Table.Cell>
              <Table.Cell>
                <Time datetime={dagRun.queued_at} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Start Date</Table.Cell>
              <Table.Cell>
                <Time datetime={dagRun.start_date} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>End Date</Table.Cell>
              <Table.Cell>
                <Time datetime={dagRun.end_date} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Data Interval Start</Table.Cell>
              <Table.Cell>
                <Time datetime={dagRun.data_interval_start} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Data Interval End</Table.Cell>
              <Table.Cell>
                <Time datetime={dagRun.data_interval_end} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Trigger Source</Table.Cell>
              <Table.Cell>{dagRun.triggered_by}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Dag Version(s)</Table.Cell>
              <Table.Cell>
                <VStack separator={<StackSeparator />}>
                  {dagRun.dag_versions.map((dagVersion) => (
                    <DagVersionDetails dagVersion={dagVersion} key={dagVersion.id} />
                  ))}
                </VStack>
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Run Config</Table.Cell>
              <Table.Cell>
                <RenderedJsonField content={dagRun.conf ?? {}} />
              </Table.Cell>
            </Table.Row>
          </Table.Body>
        </Table.Root>
      )}
    </Box>
  );
};
