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
import { Box, Flex, Heading, HStack } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";

import { DagDeactivatedBanner } from "./DagDeactivatedBanner";

type Props = {
  readonly actions?: ReactNode;
  readonly icon: ReactNode;
  readonly state?: TaskInstanceState | null;
  readonly stats: Array<{ key?: string; label: string; value: ReactNode | string }>;
  readonly subTitle?: ReactNode | string;
  readonly title: ReactNode | string;
  readonly type: "asset" | "dag" | "dagRun" | "task" | "taskGroup" | "taskInstance";
};

export const HeaderCard = ({ actions, icon, state, stats, subTitle, title, type }: Props) => {
  const { t: translate } = useTranslation();

  return (
    <Box bg="bg.muted" borderRadius="md" data-testid="header-card" flexShrink={0} overflow="hidden" px={3}>
      <DagDeactivatedBanner />
      <div>
        <Flex alignItems="center" flexWrap="wrap" justifyContent="space-between" my={2}>
          <Flex alignItems="center" flexWrap="wrap" gap={2}>
            {icon === undefined ? null : (
              <Box
                alignItems="center"
                bg="brand.muted"
                borderRadius="full"
                boxSize={10}
                color="fg.muted"
                display="flex"
                fontSize="xl"
                justifyContent="center"
              >
                {icon}
              </Box>
            )}
            <div>
              <Box color="fg.muted" fontSize="xs" fontWeight="normal" lineHeight="1">
                {translate(`common:${type}_one`)}
              </Box>
              <Heading fontSize="md" fontWeight="medium" lineHeight="1" mt={1}>
                {title}
              </Heading>
            </div>
            {subTitle === undefined ? null : <Box fontSize="md">{subTitle}</Box>}
            {state === undefined ? undefined : (
              <StateBadge state={state}>{state ? translate(`common:states.${state}`) : undefined}</StateBadge>
            )}
          </Flex>
          <HStack gap={1}>{actions}</HStack>
        </Flex>

        <HStack alignItems="flex-start" flexWrap="wrap" gap={6} my={3}>
          {stats.map((stat) => (
            <Box data-testid="stat" key={stat.key ?? stat.label}>
              <Box
                color="fg.muted"
                fontSize="xs"
                fontWeight="medium"
                lineHeight="1"
                textTransform="uppercase"
              >
                {stat.label}
              </Box>
              <Box fontSize="sm" mt={1}>
                {stat.value}
              </Box>
            </Box>
          ))}
        </HStack>
      </div>
    </Box>
  );
};
