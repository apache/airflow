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
import { Box, Flex, Table } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useOutletContext, useParams } from "react-router-dom";

import { useTaskServiceGetTask } from "openapi/queries";
import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { getDuration } from "src/utils";

export const Details = () => {
  const { dagId = "", taskId = "" } = useParams();
  const { t: translate } = useTranslation("common");

  // The aggregate summary (per-state counts, dates) is streamed once by the parent page and
  // shared through the router outlet, so this tab does not re-open the TI summaries stream.
  const taskInstance = useOutletContext<LightGridTaskInstanceSummary | undefined>();

  const { data: task } = useTaskServiceGetTask({ dagId, taskId }, undefined, { enabled: Boolean(taskId) });

  const childStates = Object.entries(taskInstance?.child_states ?? {});

  return (
    <Box p={2}>
      <Table.Root striped>
        <Table.Body>
          <Table.Row>
            <Table.Cell>{translate("overallStatus")}</Table.Cell>
            <Table.Cell>
              <Flex alignItems="center" gap={1}>
                <StateBadge state={taskInstance?.state} />
                {taskInstance?.state ?? translate("states.no_status")}
              </Flex>
            </Table.Cell>
          </Table.Row>
          {childStates.map(([state, count]) => (
            <Table.Row key={state}>
              <Table.Cell>{translate("total", { state: translate(`states.${state}`) })}</Table.Cell>
              <Table.Cell>
                <Flex alignItems="center" gap={2}>
                  <Box
                    bg={`${state}.solid`}
                    border="1px solid"
                    borderColor="border.emphasized"
                    borderRadius="2px"
                    height="10px"
                    width="10px"
                  />
                  {count}
                </Flex>
              </Table.Cell>
            </Table.Row>
          ))}
          <Table.Row>
            <Table.Cell>{translate("taskId")}</Table.Cell>
            <Table.Cell>{taskId}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("task.operator")}</Table.Cell>
            <Table.Cell>{task?.operator_name}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("task.triggerRule")}</Table.Cell>
            <Table.Cell>{task?.trigger_rule}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("dagDetails.owner")}</Table.Cell>
            <Table.Cell>{task?.owner}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("task.retries")}</Table.Cell>
            <Table.Cell>{task?.retries}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.pool")}</Table.Cell>
            <Table.Cell>{task?.pool}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.poolSlots")}</Table.Cell>
            <Table.Cell>{task?.pool_slots}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.queue")}</Table.Cell>
            <Table.Cell>{task?.queue}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.priorityWeight")}</Table.Cell>
            <Table.Cell>{task?.priority_weight}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("task.dependsOnPast")}</Table.Cell>
            <Table.Cell>{task === undefined ? undefined : String(task.depends_on_past)}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("task.waitForDownstream")}</Table.Cell>
            <Table.Cell>{task === undefined ? undefined : String(task.wait_for_downstream)}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("startDate")}</Table.Cell>
            <Table.Cell>
              <Time datetime={taskInstance?.min_start_date} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("endDate")}</Table.Cell>
            <Table.Cell>
              <Time datetime={taskInstance?.max_end_date} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("duration")}</Table.Cell>
            <Table.Cell>{getDuration(taskInstance?.min_start_date, taskInstance?.max_end_date)}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.dagVersion")}</Table.Cell>
            <Table.Cell>{taskInstance?.dag_version_number}</Table.Cell>
          </Table.Row>
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
