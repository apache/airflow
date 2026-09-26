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
import { HStack, Table, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { TeamName } from "src/components/TeamName";
import Time from "src/components/Time";

import { useConfig } from "src/queries/useConfig";

/**
 * One live component replica, normalised across the three per-component instance payloads so the
 * table does not have to know which ``latest_*_heartbeat`` field the endpoint used. ``teamNames``
 * and ``bundleNames`` are left undefined for components that do not report them at all. There is no
 * per-instance status: the endpoint only lists replicas that are running, and how healthy the set of
 * them is together is what the component badge shows.
 */
export type HealthInstance = {
  readonly bundleNames?: Array<string> | null;
  readonly hostname: string | null;
  readonly latestHeartbeat: string | null;
  readonly teamNames?: Array<string> | null;
};

type Props = {
  readonly instances: Array<HealthInstance>;
};

export const HealthInstances = ({ instances }: Props) => {
  const { t: translate } = useTranslation("dashboard");
  const isMultiTeam = Boolean(useConfig("multi_team"));
  // Both columns only apply to one component each, and even there every instance may report
  // nothing — an all-empty column is noise, so it is dropped rather than rendered blank.
  const showTeam = isMultiTeam && instances.some((instance) => Boolean(instance.teamNames?.length));
  const showBundles = instances.some((instance) => Boolean(instance.bundleNames?.length));

  return (
    <Table.Root size="sm">
      <Table.Header>
        <Table.Row>
          <Table.ColumnHeader>{translate("health.instances.hostname")}</Table.ColumnHeader>
          <Table.ColumnHeader>{translate("health.lastHeartbeat")}</Table.ColumnHeader>
          {showTeam ? (
            <Table.ColumnHeader>{translate("health.instances.team")}</Table.ColumnHeader>
          ) : undefined}
          {showBundles ? (
            <Table.ColumnHeader>{translate("health.instances.bundles")}</Table.ColumnHeader>
          ) : undefined}
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {instances.map((instance) => (
          <Table.Row key={`${instance.hostname ?? ""}-${instance.latestHeartbeat ?? ""}`}>
            <Table.Cell>
              <Text fontFamily="mono">
                {instance.hostname ?? translate("health.instances.unknownHostname")}
              </Text>
            </Table.Cell>
            <Table.Cell>
              <Time datetime={instance.latestHeartbeat} />
            </Table.Cell>
            {showTeam ? (
              <Table.Cell>
                <HStack gap={2} wrap="wrap">
                  {instance.teamNames?.map((teamName) => (
                    <TeamName key={teamName} teamName={teamName} />
                  ))}
                </HStack>
              </Table.Cell>
            ) : undefined}
            {showBundles ? <Table.Cell>{instance.bundleNames?.join(", ")}</Table.Cell> : undefined}
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Root>
  );
};
