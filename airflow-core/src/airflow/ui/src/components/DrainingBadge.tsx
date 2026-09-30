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
import type { BadgeProps } from "@chakra-ui/react";
import { Button, HStack, Stack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdHourglassTop } from "react-icons/md";

import { Popover } from "src/system-components";

import { useTogglePause } from "src/queries/useTogglePause";

import { StateBadge } from "./StateBadge";

type Props = {
  /** Enables the drain actions. Omit in list contexts that only report the state. */
  readonly dagId?: string;
} & BadgeProps;

export const DrainingBadge = ({ dagId, ...props }: Props) => {
  const { t: translate } = useTranslation("dags");
  const { isPending, mutate } = useTogglePause({ dagId: dagId ?? "" });

  return (
    <Popover.Root lazyMount unmountOnExit>
      <Popover.Trigger asChild>
        <StateBadge
          as="button"
          colorPalette="warning"
          cursor="pointer"
          data-testid="draining-badge"
          variant="subtle"
          {...props}
        >
          <MdHourglassTop />
          {translate("schedulingState.draining")}
        </StateBadge>
      </Popover.Trigger>
      <Popover.Content maxW="340px" width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          <Stack gap={2}>
            <Text data-testid="draining-explanation" fontSize="sm">
              {translate("schedulingState.drainingBadgeTooltip")}
            </Text>
            {dagId === undefined ? undefined : (
              <HStack gap={2}>
                <Button
                  data-testid="banner-cancel-drain"
                  loading={isPending}
                  onClick={() => mutate({ dagId, requestBody: { scheduling_state: "active" } })}
                  size="xs"
                  variant="outline"
                >
                  {translate("schedulingActions.cancelDrain")}
                </Button>
                <Button
                  data-testid="banner-pause-now"
                  loading={isPending}
                  onClick={() => mutate({ dagId, requestBody: { scheduling_state: "paused" } })}
                  size="xs"
                  variant="outline"
                >
                  {translate("schedulingActions.pauseNow")}
                </Button>
              </HStack>
            )}
          </Stack>
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
