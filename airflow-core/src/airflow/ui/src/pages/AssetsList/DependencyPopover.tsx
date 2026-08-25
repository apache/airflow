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
import { Button, HStack, Icon, VisuallyHidden } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiUsers } from "react-icons/fi";

import type { DagScheduleAssetReference, TaskOutletAssetReference } from "openapi/requests/types.gen";
import { TeamName } from "src/components/TeamName";
import { Popover, RouterLink, Tooltip } from "src/components/ui";
import { useShowTeam } from "src/hooks/useShowTeam";

type Props = {
  readonly dependencies: Array<DagScheduleAssetReference | TaskOutletAssetReference>;
  readonly type: "Dag" | "Task";
};

// Elsewhere a team sits under a "Team" column header or detail-row label; the popover has no such
// heading, so the icon marks the link as a team rather than a second Dag link. ``FiUsers`` is the
// same icon the teams filter uses.
const IconTeamName = ({ teamName }: { readonly teamName?: string | null }) => {
  const { t: translate } = useTranslation("common");
  const showTeam = useShowTeam(teamName);

  if (!showTeam) {
    return undefined;
  }

  return (
    <Tooltip content={translate("dagDetails.team")}>
      <HStack gap={1}>
        <Icon color="fg.muted">
          <FiUsers />
        </Icon>
        {/* The icon is the only cue that this link is a team rather than another Dag, and Chakra
            hides icons from assistive tech, so the label is announced separately. */}
        <VisuallyHidden>{translate("dagDetails.team")}</VisuallyHidden>
        <TeamName teamName={teamName} />
      </HStack>
    </Tooltip>
  );
};

export const DependencyPopover = ({ dependencies, type }: Props) => {
  const { t: translate } = useTranslation();
  const dependencyKey = type.toLowerCase() as "dag" | "task";

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount unmountOnExit>
      <Popover.Trigger asChild disabled={dependencies.length === 0}>
        <Button variant="outline">
          {dependencies.length} {translate(dependencyKey, { count: dependencies.length })}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          {dependencies.map((dependency) => {
            let key = dependency.dag_id;
            let link = `/dags/${dependency.dag_id}`;
            let label = dependency.dag_id;

            if (type === "Task") {
              const dep = dependency as TaskOutletAssetReference;

              key = `${dep.dag_id}-${dep.task_id}`;
              link = `/dags/${dep.dag_id}/tasks/${dep.task_id}`;
              label = `${dep.dag_id}.${dep.task_id}`;
            }

            return (
              <HStack gap={2} justifyContent="space-between" key={key} py={2}>
                <RouterLink to={link}>{label}</RouterLink>
                <IconTeamName teamName={dependency.team_name} />
              </HStack>
            );
          })}
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
