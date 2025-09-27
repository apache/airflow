/* eslint-disable i18next/no-literal-string */

/* eslint-disable react/jsx-max-depth */

/* eslint-disable @typescript-eslint/no-unsafe-assignment */

/* eslint-disable @typescript-eslint/no-explicit-any */

/* eslint-disable max-lines */

/* eslint-disable react/no-unescaped-entities */

/* eslint-disable @stylistic/spaced-comment */

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
import {
  Box,
  Collapsible,
  Heading,
  HStack,
  Progress,
  ProgressCircle,
  Spinner,
  Text,
  VStack,
} from "@chakra-ui/react";

import { StateBadge } from "src/components/StateBadge";
import { StateIcon } from "src/components/StateIcon";

type ProgressTaskStatesSectionProps = {
  readonly isProgressOpen: boolean;
  readonly isStatesOpen: boolean;
  readonly onProgressToggle: () => void;
  readonly onStatesToggle: () => void;
  readonly progressValue: number;
  readonly setProgressValue: (value: number) => void;
};

export const ProgressTaskStatesSection = ({
  isProgressOpen,
  isStatesOpen,
  onProgressToggle,
  onStatesToggle,
  progressValue,
}: ProgressTaskStatesSectionProps) => {
  const taskStates = [
    { count: 150, label: "Success", state: "success" },
    { count: 25, label: "Running", state: "running" },
    { count: 8, label: "Failed", state: "failed" },
    { count: 3, label: "Upstream Failed", state: "upstream_failed" },
    { count: 45, label: "Skipped", state: "skipped" },
    { count: 5, label: "Up for Retry", state: "up_for_retry" },
    { count: 2, label: "Up for Reschedule", state: "up_for_reschedule" },
    { count: 12, label: "Queued", state: "queued" },
    { count: 30, label: "Scheduled", state: "scheduled" },
    { count: 7, label: "Deferred", state: "deferred" },
    { count: 1, label: "Removed", state: "removed" },
  ];

  return (
    <VStack align="stretch" gap={8}>
      {/* Progress Section */}
      <Box id="progress">
        <Collapsible.Root onOpenChange={onProgressToggle} open={isProgressOpen}>
          <Collapsible.Trigger
            _hover={{ bg: "bg.subtle" }}
            borderColor={isProgressOpen ? "brand.emphasized" : "border.muted"}
            borderWidth="1px"
            cursor="pointer"
            paddingX="6"
            paddingY="4"
            transition="all 0.2s"
            width="full"
          >
            <HStack justify="space-between" width="full">
              <VStack align="flex-start" gap="1">
                <Heading size="xl">Progress</Heading>
                <Text color="fg.muted" fontSize="sm">
                  Progress indicators and loading states
                </Text>
              </VStack>
              <Text color="brand.solid" fontSize="lg">
                {isProgressOpen ? "−" : "+"}
              </Text>
            </HStack>
          </Collapsible.Trigger>
          <Collapsible.Content>
            <Box borderColor="border.muted" borderTop="none" borderWidth="1px" padding="6">
              <VStack align="stretch" gap={6}>
                {/* Linear Progress */}
                <VStack align="stretch" gap={4}>
                  <Heading size="lg">Linear Progress</Heading>
                  <VStack align="stretch" gap={4}>
                    <Box>
                      <Text fontSize="sm" fontWeight="semibold">
                        Default Progress ({progressValue}%)
                      </Text>
                      <Progress.Root value={progressValue}>
                        <Progress.Track>
                          <Progress.Range />
                        </Progress.Track>
                      </Progress.Root>
                    </Box>

                    <Box>
                      <Text fontSize="sm" fontWeight="semibold">
                        Colored Progress
                      </Text>
                      <VStack align="stretch" gap={2}>
                        <Progress.Root colorPalette="green" value={85}>
                          <Progress.Track>
                            <Progress.Range />
                          </Progress.Track>
                        </Progress.Root>
                        <Progress.Root colorPalette="yellow" value={60}>
                          <Progress.Track>
                            <Progress.Range />
                          </Progress.Track>
                        </Progress.Root>
                        <Progress.Root colorPalette="red" value={30}>
                          <Progress.Track>
                            <Progress.Range />
                          </Progress.Track>
                        </Progress.Root>
                      </VStack>
                    </Box>
                  </VStack>
                </VStack>
              </VStack>

              {/* Circular Progress */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Circular Progress</Heading>
                <HStack gap={6} wrap="wrap">
                  <VStack gap={2}>
                    <ProgressCircle.Root size="lg" value={progressValue}>
                      <ProgressCircle.Circle>
                        <ProgressCircle.Track />
                        <ProgressCircle.Range />
                      </ProgressCircle.Circle>
                      <ProgressCircle.ValueText />
                    </ProgressCircle.Root>
                    <Text fontSize="sm">Default</Text>
                  </VStack>

                  <VStack gap={2}>
                    <ProgressCircle.Root colorPalette="green" size="lg" value={85}>
                      <ProgressCircle.Circle>
                        <ProgressCircle.Track />
                        <ProgressCircle.Range />
                      </ProgressCircle.Circle>
                      <ProgressCircle.ValueText />
                    </ProgressCircle.Root>
                    <Text fontSize="sm">Success</Text>
                  </VStack>

                  <VStack gap={2}>
                    <ProgressCircle.Root colorPalette="red" size="lg" value={30}>
                      <ProgressCircle.Circle>
                        <ProgressCircle.Track />
                        <ProgressCircle.Range />
                      </ProgressCircle.Circle>
                      <ProgressCircle.ValueText />
                    </ProgressCircle.Root>
                    <Text fontSize="sm">Error</Text>
                  </VStack>
                </HStack>
                {/* Spinners */}
                <VStack align="stretch" gap={4}>
                  <Heading size="lg">Loading Spinners</Heading>
                  <HStack gap={6} wrap="wrap">
                    <VStack gap={2}>
                      <Spinner size="xs" />
                      <Text fontSize="sm">Extra Small</Text>
                    </VStack>
                    <VStack gap={2}>
                      <Spinner size="sm" />
                      <Text fontSize="sm">Small</Text>
                    </VStack>
                    <VStack gap={2}>
                      <Spinner size="md" />
                      <Text fontSize="sm">Medium</Text>
                    </VStack>
                    <VStack gap={2}>
                      <Spinner size="lg" />
                      <Text fontSize="sm">Large</Text>
                    </VStack>
                    <VStack gap={2}>
                      <Spinner size="xl" />
                      <Text fontSize="sm">Extra Large</Text>
                    </VStack>
                  </HStack>
                </VStack>
              </VStack>
            </Box>
          </Collapsible.Content>
        </Collapsible.Root>
      </Box>

      {/* Task Instance States Section */}
      <Box id="states">
        <Collapsible.Root onOpenChange={onStatesToggle} open={isStatesOpen}>
          <Collapsible.Trigger
            _hover={{ bg: "bg.subtle" }}
            borderColor={isStatesOpen ? "brand.emphasized" : "border.muted"}
            borderWidth="1px"
            cursor="pointer"
            paddingX="6"
            paddingY="4"
            transition="all 0.2s"
            width="full"
          >
            <HStack justify="space-between" width="full">
              <VStack align="flex-start" gap="1">
                <Heading size="xl">Task Instance States</Heading>
                <Text color="fg.muted" fontSize="sm">
                  All possible task execution states
                </Text>
              </VStack>
              <Text color="brand.solid" fontSize="lg">
                {isStatesOpen ? "−" : "+"}
              </Text>
            </HStack>
          </Collapsible.Trigger>
          <Collapsible.Content>
            <Box borderColor="border.muted" borderTop="none" borderWidth="1px" padding="6">
              <VStack align="stretch" gap={4}>
                {taskStates.map(({ count, label, state }) => {
                  const totalCount = taskStates.reduce((sum, item) => sum + item.count, 0);
                  const percentage = totalCount === 0 ? 0 : ((count / totalCount) * 100).toFixed(1);
                  const barWidth = totalCount === 0 ? 0 : (count / totalCount) * 100;
                  const remainingWidth = 100 - barWidth;

                  return (
                    <VStack align="stretch" bg="bg.subtle" gap={2} key={state} p={3}>
                      {/* Top row: State info and count */}
                      <HStack justify="space-between">
                        <HStack gap={3}>
                          <StateIcon state={state as any} />
                          <StateBadge state={state as any} />
                          <Text fontSize="sm">{label}</Text>
                        </HStack>
                        <HStack gap={2}>
                          <Text color="fg.muted" fontSize="sm" fontWeight="semibold">
                            {count}
                          </Text>
                          <Text color="fg.muted" fontSize="xs">
                            ({percentage}%)
                          </Text>
                        </HStack>
                      </HStack>

                      {/* Bottom row: Progress bar */}
                      <HStack gap={0} height="8px" width="100%">
                        <Box
                          bg={`${state}.solid`}
                          borderLeftRadius="md"
                          borderRightRadius={count === totalCount ? "md" : "none"}
                          height="100%"
                          minWidth={count > 0 ? "2px" : "0px"}
                          width={`${barWidth}%`}
                        />
                        <Box
                          bg="bg.emphasized"
                          borderLeftRadius={count === 0 ? "md" : "none"}
                          borderRightRadius="md"
                          height="100%"
                          width={`${remainingWidth}%`}
                        />
                      </HStack>
                    </VStack>
                  );
                })}
              </VStack>
            </Box>
          </Collapsible.Content>
        </Collapsible.Root>
      </Box>
    </VStack>
  );
};
