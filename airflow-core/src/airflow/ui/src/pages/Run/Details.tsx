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
import { Flex, HStack, StackSeparator, Table, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
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
  const { t: translate } = useTranslation(["common", "components"]);
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

  if (!dagRun) {
    return undefined;
  }

  return (
    <Table.Root striped>
      <Table.Body>
        <Table.Row>
          <Table.Cell>{translate("state")}</Table.Cell>
          <Table.Cell>
            <Flex gap={1}>
              <StateBadge state={dagRun.state} />
              {translate(`common:states.${dagRun.state}`)}
            </Flex>
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("runId")}</Table.Cell>
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
          <Table.Cell>{translate("dagRun.runType")}</Table.Cell>
          <Table.Cell>
            <HStack>
              <RunTypeIcon runType={dagRun.run_type} />
              <Text>{dagRun.run_type}</Text>
            </HStack>
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("duration")}</Table.Cell>
          <Table.Cell>{getDuration(dagRun.start_date, dagRun.end_date)}</Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("dagRun.lastSchedulingDecision")}</Table.Cell>
          <Table.Cell>
            <Time datetime={dagRun.last_scheduling_decision} />
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("dagRun.queuedAt")}</Table.Cell>
          <Table.Cell>
            <Time datetime={dagRun.queued_at} />
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("startDate")}</Table.Cell>
          <Table.Cell>
            <Time datetime={dagRun.start_date} />
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("endDate")}</Table.Cell>
          <Table.Cell>
            <Time datetime={dagRun.end_date} />
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("dagRun.dataIntervalStart")}</Table.Cell>
          <Table.Cell>
            <Time datetime={dagRun.data_interval_start} />
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("dagRun.dataIntervalEnd")}</Table.Cell>
          <Table.Cell>
            <Time datetime={dagRun.data_interval_end} />
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("dagRun.triggeredBy")}</Table.Cell>
          <Table.Cell>{dagRun.triggered_by}</Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("dagRun.triggeringUser")}</Table.Cell>
          <Table.Cell>{dagRun.triggering_user_name}</Table.Cell>
        </Table.Row>
        {dagRun.bundle_version !== null && (
          <Table.Row>
            <Table.Cell>{translate("components:versionDetails.bundleVersion")}</Table.Cell>
            <Table.Cell>{dagRun.bundle_version}</Table.Cell>
          </Table.Row>
        )}
        <Table.Row>
          <Table.Cell>{translate("dagRun.dagVersions")}</Table.Cell>
          <Table.Cell>
            <VStack separator={<StackSeparator />}>
              {dagRun.dag_versions.map((dagVersion) => (
                <DagVersionDetails dagVersion={dagVersion} key={dagVersion.id} />
              ))}
            </VStack>
          </Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>{translate("dagRun.conf")}</Table.Cell>
          <Table.Cell>
            <RenderedJsonField content={dagRun.conf ?? {}} />
          </Table.Cell>
        </Table.Row>
      </Table.Body>
    </Table.Root>
  );
};
