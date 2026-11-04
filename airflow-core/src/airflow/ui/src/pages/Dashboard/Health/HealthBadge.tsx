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
import { Heading, Skeleton, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Popover, Tooltip } from "src/system-components";

import Time from "src/components/Time";

import { type HealthInstance, HealthInstances } from "./HealthInstances";
import { HealthStateBadge } from "./HealthStateBadge";
import { DEGRADED, healthTranslationKey } from "./healthStatus";

type Props = {
  /** Why this component reports "degraded"; components that can never report it omit it. */
  readonly degradedHint?: string;
  readonly instances?: Array<HealthInstance> | null;
  readonly isLoading: boolean;
  readonly latestHeartbeat?: string | null;
  readonly status?: string | null;
  readonly title: string;
};

export const HealthBadge = ({
  degradedHint,
  instances,
  isLoading,
  latestHeartbeat,
  status,
  title,
}: Props) => {
  const { t: translate } = useTranslation("dashboard");

  if (isLoading) {
    return <Skeleton borderRadius="full" height={8} width={24} />;
  }

  const hasInstances = instances !== null && instances !== undefined && instances.length > 0;

  // A tooltip trigger cannot double as the popover trigger: both are ``asChild`` and the tooltip
  // wins the merge of ``id`` and the ``data-scope``/``data-part`` pair, which leaves the popover
  // positioner without an anchor and drops the panel in the corner of the viewport. Components
  // that report instances therefore rely on the popover alone, which repeats the status and gives
  // a per-instance heartbeat, so nothing the tooltip showed is lost.
  if (!hasInstances) {
    return (
      <Tooltip
        content={
          <div>
            <Text>
              {translate("health.status")}
              {": "}
              {translate(healthTranslationKey(status))}
            </Text>
            <Text hidden={latestHeartbeat === undefined}>
              {translate("health.lastHeartbeat")}
              {": "}
              <Time datetime={latestHeartbeat} />
            </Text>
          </div>
        }
      >
        <HealthStateBadge size="lg" status={status}>
          {title}
        </HealthStateBadge>
      </Tooltip>
    );
  }

  return (
    <Popover.Root lazyMount unmountOnExit>
      <Popover.Trigger asChild>
        <HealthStateBadge as="button" cursor="pointer" size="lg" status={status}>
          {title}
        </HealthStateBadge>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          <Heading mb={2} size="xs">
            {translate("health.instances.title", {
              count: instances.length,
              status: translate(healthTranslationKey(status)),
              title,
            })}
          </Heading>
          {/* Every listed replica is running, so a degraded badge is otherwise unexplained: the
              missing coverage is work no replica has picked up rather than a replica that is down. */}
          {status === DEGRADED && degradedHint !== undefined ? (
            <Text color="fg.muted" maxWidth="xs" mb={2}>
              {degradedHint}
            </Text>
          ) : undefined}
          <HealthInstances instances={instances} />
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
