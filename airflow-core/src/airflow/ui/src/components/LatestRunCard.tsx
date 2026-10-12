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

import { Box, Flex, Heading, Skeleton, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link, type To } from "react-router-dom";

import type { TaskInstanceState } from "openapi/requests/types.gen";

import { StateBadge } from "src/components/StateBadge";

const CARD_WIDTH = "480px";

export const LatestRunCardSkeleton = () => <Skeleton borderRadius="md" height="56px" width={CARD_WIDTH} />;

type Props = {
  readonly icon: ReactNode;
  readonly label: string;
  readonly state?: TaskInstanceState | null;
  /** Task-instance state badges for the run, rendered under the title. Must not contain links. */
  readonly taskStates?: ReactNode;
  readonly title: string;
  readonly to: To;
};

/**
 * The run's header row, as a shortcut onto the run itself. Everything the header card shows
 * beyond this is a click away, so the card stays a single glanceable line.
 */
export const LatestRunCard = ({ icon, label, state, taskStates, title, to }: Props) => {
  const { t: translate } = useTranslation("common");

  return (
    <Box asChild maxWidth="100%" width={CARD_WIDTH}>
      <Link data-testid="latest-run-card" to={to}>
        <VStack
          _hover={{ bg: "bg.subtle" }}
          alignItems="stretch"
          borderColor="border.emphasized"
          borderRadius="md"
          borderWidth={1}
          gap={2}
          px={3}
          py={2}
          transition="background-color 0.2s"
        >
          <Flex alignItems="center" gap={2}>
            <Box
              alignItems="center"
              bg="brand.muted"
              borderRadius="full"
              boxSize={8}
              color="fg.muted"
              display="flex"
              flexShrink={0}
              fontSize="md"
              justifyContent="center"
            >
              {icon}
            </Box>
            <Box minWidth={0}>
              <Box color="fg.muted" fontSize="xs" fontWeight="medium" lineHeight="1">
                {label}
              </Box>
              <Heading fontSize="md" fontWeight="medium" lineHeight="1.3" title={title} truncate>
                {title}
              </Heading>
            </Box>
            <StateBadge flexShrink={0} ml="auto" state={state}>
              {state === null || state === undefined ? undefined : translate(`states.${state}`)}
            </StateBadge>
          </Flex>
          {taskStates}
        </VStack>
      </Link>
    </Box>
  );
};
