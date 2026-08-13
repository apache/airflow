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
import { Button, Flex, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { DagRunAssetReference, DagRunState } from "openapi/requests/types.gen";
import { Popover, RouterLink } from "src/components/ui";

import { StateBadge } from "../StateBadge";

type Props = {
  readonly dagRuns?: Array<DagRunAssetReference>;
};

const DagRunGroup = ({
  dagRuns,
  prefix,
}: {
  readonly dagRuns: Array<DagRunAssetReference>;
  readonly prefix: string;
}) => {
  const { t: translate } = useTranslation("common");

  return dagRuns.length === 1 ? (
    <Flex gap={1}>
      <Text>{`${prefix} ${translate("dagRun_one")}`}: </Text>
      <StateBadge state={dagRuns[0]?.state as DagRunState} />
      <RouterLink to={`/dags/${dagRuns[0]?.dag_id}/runs/${dagRuns[0]?.run_id}`}>
        {dagRuns[0]?.dag_id}
      </RouterLink>
    </Flex>
  ) : (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount unmountOnExit>
      <Popover.Trigger asChild>
        <Button variant="outline">
          {`${dagRuns.length} ${prefix} ${translate("dagRun_other", { count: dagRuns.length })}`}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          {dagRuns.map((dagRun) => (
            <Flex gap={1} key={`${dagRun.dag_id}-${dagRun.run_id}`} my={2}>
              <StateBadge state={dagRun.state as DagRunState} />
              <RouterLink to={`/dags/${dagRun.dag_id}/runs/${dagRun.run_id}`}>{dagRun.dag_id}</RouterLink>
            </Flex>
          ))}
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};

export const TriggeredRuns = ({ dagRuns }: Props) => {
  const { t: translate } = useTranslation("common");

  if (dagRuns === undefined || dagRuns.length === 0) {
    return undefined;
  }

  // An asset event is linked to every run that consumed it, but only a run's most recent consumed
  // event triggered it (backend flag). The rest were merely included, so label them accordingly.
  const triggeredRuns = dagRuns.filter((dagRun) => dagRun.triggering);
  const includedRuns = dagRuns.filter((dagRun) => !dagRun.triggering);

  return (
    <Flex direction="column" gap={1}>
      {triggeredRuns.length > 0 ? (
        <DagRunGroup dagRuns={triggeredRuns} prefix={translate("triggered")} />
      ) : undefined}
      {includedRuns.length > 0 ? (
        <DagRunGroup dagRuns={includedRuns} prefix={translate("includedIn")} />
      ) : undefined}
    </Flex>
  );
};
