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
import type { ReactNode } from "react";

import { Box, Flex, Heading, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { TaskInstanceState } from "openapi/requests/types.gen";

import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";

export type HeaderCardType = "asset" | "dag" | "dagBundle" | "dagRun" | "task" | "taskGroup" | "taskInstance";

type Props = {
  readonly actions?: ReactNode;
  /** Why this object could not be loaded, shown between its name and its stats. */
  readonly error?: unknown;
  /**
   * Offer to dismiss the error. Only where the card has stats of its own: on a card that could not
   * load, the error is all it says, and closing it leaves an empty card with no reason given.
   */
  readonly errorClosable?: boolean;
  readonly icon: ReactNode;
  readonly state?: TaskInstanceState | null;
  readonly stats: Array<{ key?: string; label: ReactNode | string; value: ReactNode | string }>;
  /** Status for the object in view, rendered beside its name. */
  readonly statusBadge?: ReactNode;
  readonly subTitle?: ReactNode | string;
  /** Overridden by the loading variant, so a skeleton is not mistaken for a loaded card. */
  readonly testId?: string;
  readonly title: ReactNode | string;
  readonly type: HeaderCardType;
};

export const HeaderCard = ({
  actions,
  error,
  errorClosable = false,
  icon,
  state,
  stats,
  statusBadge,
  subTitle,
  testId = "header-card",
  title,
  type,
}: Props) => {
  const { t: translate } = useTranslation();

  return (
    <Box bg="bg.muted" borderRadius="md" data-testid={testId} flexShrink={0} overflow="hidden" px={3}>
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
            {statusBadge}
            {subTitle === undefined ? null : <Box fontSize="md">{subTitle}</Box>}
            {state === undefined ? undefined : (
              <StateBadge state={state}>{state ? translate(`common:states.${state}`) : undefined}</StateBadge>
            )}
          </Flex>
          <HStack gap={1}>{actions}</HStack>
        </Flex>

        {Boolean(error) ? (
          <Box mb={3}>
            <ErrorAlert closable={errorClosable} error={error} />
          </Box>
        ) : undefined}

        <HStack alignItems="flex-start" flexWrap="wrap" gap={6} my={3}>
          {stats.map((stat, index) => (
            // A label is a string for every real stat; the loading variant passes a skeleton.
            <Box data-testid="stat" key={stat.key ?? (typeof stat.label === "string" ? stat.label : index)}>
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
