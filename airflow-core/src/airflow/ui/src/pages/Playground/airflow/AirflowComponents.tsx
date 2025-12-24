/* eslint-disable i18next/no-literal-string */

/* eslint-disable react/jsx-max-depth */

/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
import { Box, Collapsible, Heading, HStack, Text, VStack } from "@chakra-ui/react";

import { HeaderCard } from "src/components/HeaderCard";
import { StateBadge } from "src/components/StateBadge";
import { StateIcon } from "src/components/StateIcon";
import { StatsCard } from "src/components/StatsCard";
import type { TaskInstanceState } from "openapi/requests/types.gen";

type AirflowComponentsProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const AirflowComponents = ({ isOpen, onToggle }: AirflowComponentsProps) => {
  const taskStates = [
    "success", "running", "failed", "queued", "skipped", 
    "upstream_failed", "up_for_retry", "up_for_reschedule", 
    "scheduled", "deferred", "removed"
  ];

  return (
    <Box id="airflow-components">
      <Collapsible.Root onOpenChange={onToggle} open={isOpen}>
        <Collapsible.Trigger
          _hover={{ bg: "bg.subtle" }}
          borderColor={isOpen ? "brand.emphasized" : "border.muted"}
          borderWidth="1px"
          cursor="pointer"
          paddingX="6"
          paddingY="4"
          transition="all 0.2s"
          width="full"
        >
          <HStack justify="space-between" width="full">
            <VStack align="flex-start" gap="1">
              <Heading size="xl">Airflow Components</Heading>
              <Text color="fg.muted" fontSize="sm">
                Airflow-specific UI components and patterns
              </Text>
            </VStack>
            <Text color="brand.solid" fontSize="lg">
              {isOpen ? "−" : "+"}
            </Text>
          </HStack>
        </Collapsible.Trigger>
        <Collapsible.Content>
          <Box borderColor="border.muted" borderTop="none" borderWidth="1px" padding="6">
            <VStack align="stretch" gap={6}>
              {/* State Components */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">State Components</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      State Badges
                    </Text>
                    <HStack gap={2} wrap="wrap">
                      {taskStates.map((state) => (
                        <StateBadge key={state} state={state as TaskInstanceState}>
                          {state.replace("_", " ")}
                        </StateBadge>
                      ))}
                    </HStack>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      State Icons
                    </Text>
                    <HStack gap={4} wrap="wrap">
                      {taskStates.map((state) => (
                        <HStack gap={2} key={state}>
                          <StateIcon state={state as TaskInstanceState} />
                          <Text fontSize="sm">{state.replace("_", " ")}</Text>
                        </HStack>
                      ))}
                    </HStack>
                  </Box>
                </VStack>
              </VStack>

              {/* Header Card */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Header Card</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Basic Header Card
                    </Text>
                    <HeaderCard
                      actions={
                        <HStack gap={2}>
                          <Text fontSize="sm" color="fg.muted">Edit</Text>
                          <Text fontSize="sm" color="fg.muted">Trigger</Text>
                          <Text fontSize="sm" color="fg.muted">Pause</Text>
                        </HStack>
                      }
                      icon="📊"
                      state="success"
                      stats={[
                        { label: "Tasks", value: "15" },
                        { label: "Duration", value: "2h 30m" },
                        { label: "Last Run", value: "2024-01-15" },
                        { label: "Next Run", value: "2024-01-16" },
                      ]}
                      subTitle="(dag_id: sample_dag)"
                      title="Sample DAG"
                    />
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Header Card with Error State
                    </Text>
                    <HeaderCard
                      actions={
                        <HStack gap={2}>
                          <Text fontSize="sm" color="fg.muted">Retry</Text>
                          <Text fontSize="sm" color="fg.muted">Unpause</Text>
                        </HStack>
                      }
                      icon="❌"
                      state="failed"
                      stats={[
                        { label: "Tasks", value: "8" },
                        { label: "Failed", value: "3" },
                        { label: "Last Run", value: "2024-01-14" },
                        { label: "Next Run", value: "Paused" },
                      ]}
                      subTitle="(dag_id: failed_dag)"
                      title="Failed DAG"
                    />
                  </Box>
                </VStack>
              </VStack>

              {/* Stats Card */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Stats Card</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Basic Stats Card
                    </Text>
                    <StatsCard
                      stats={[
                        { label: "Success Rate", value: "95.2%" },
                        { label: "Avg Duration", value: "1h 45m" },
                        { label: "Total Runs", value: "1,234" },
                        { label: "Last Success", value: "2h ago" },
                      ]}
                      title="DAG Performance"
                    />
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Stats Card with Custom Colors
                    </Text>
                    <StatsCard
                      stats={[
                        { color: "blue", label: "Running", value: "5" },
                        { color: "green", label: "Success", value: "142" },
                        { color: "red", label: "Failed", value: "3" },
                        { color: "gray", label: "Skipped", value: "12" },
                      ]}
                      title="Task Metrics"
                    />
                  </Box>
                </VStack>
              </VStack>

              {/* Individual Stats */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Individual Stats</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Stat Components
                    </Text>
                    <HStack gap={6} wrap="wrap">
                      <VStack align="center" gap={1}>
                        <Text fontSize="sm" color="fg.muted">Total DAGs</Text>
                        <Text fontSize="lg" fontWeight="semibold">47</Text>
                      </VStack>
                      <VStack align="center" gap={1}>
                        <Text fontSize="sm" color="fg.muted">Active DAGs</Text>
                        <Text fontSize="lg" fontWeight="semibold">42</Text>
                      </VStack>
                      <VStack align="center" gap={1}>
                        <Text fontSize="sm" color="fg.muted">Paused DAGs</Text>
                        <Text fontSize="lg" fontWeight="semibold">5</Text>
                      </VStack>
                      <VStack align="center" gap={1}>
                        <Text fontSize="sm" color="fg.muted">Success Rate</Text>
                        <Text fontSize="lg" fontWeight="semibold">94.8%</Text>
                      </VStack>
                    </HStack>
                  </Box>
                </VStack>
              </VStack>

              {/* Task Instance States */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Task Instance States</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      All possible task execution states with counts and percentages
                    </Text>
                    <VStack align="stretch" gap={4}>
                      {[
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
                      ].map(({ count, label, state }) => {
                        const totalCount = 288; // Sum of all counts
                        const percentage = totalCount === 0 ? 0 : ((count / totalCount) * 100).toFixed(1);
                        const barWidth = totalCount === 0 ? 0 : (count / totalCount) * 100;
                        const remainingWidth = 100 - barWidth;

                        return (
                          <VStack align="stretch" bg="bg.subtle" gap={2} key={state} p={3}>
                            {/* Top row: State info and count */}
                            <HStack justify="space-between">
                              <HStack gap={3}>
                                <StateIcon state={state as TaskInstanceState} />
                                <StateBadge state={state as TaskInstanceState} />
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
                </VStack>
              </VStack>
            </VStack>
          </Box>
        </Collapsible.Content>
      </Collapsible.Root>
    </Box>
  );
};
