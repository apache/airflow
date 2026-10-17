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
import { Box, Flex, Text } from "@chakra-ui/react";

import { StateIcon } from "src/components/StateIcon";

import { COLORS, COPY, isPast, type Phase, ring } from "./stage";

// Each task runs during its own phase, then stays successful.
const getTaskState = (phase: Phase, taskPhase: Phase): keyof typeof COPY.states => {
  if (phase === taskPhase) {
    return "running";
  }

  return isPast(phase, taskPhase) ? "success" : "queued";
};

/**
 * The scene, run as a Dag: each task runs while its part of the show happens on stage.
 */
export const MiniDag = ({ phase, taskId }: { readonly phase: Phase; readonly taskId: string }) => {
  const tasks: Array<{ id: string; state: keyof typeof COPY.states }> = [
    { id: "dim_the_lights", state: getTaskState(phase, "dim") },
    { id: "drop_the_needle", state: getTaskState(phase, "needle") },
    { id: taskId, state: getTaskState(phase, "show") },
  ];

  return (
    <>
      {/* Only the finished run is announced: per-task updates would talk over the opening music. */}
      <Text aria-live="polite" as="output" srOnly>
        {phase === "done" ? `${taskId}: ${COPY.states.success}` : undefined}
      </Text>
      <Flex
        alignItems="center"
        aria-label={COPY.dag}
        as="ol"
        fontFamily="mono"
        fontSize={{ base: "xs", lg: "md", md: "sm" }}
        gap={{ base: 2, lg: 4, md: 3 }}
        listStyleType="none"
        minWidth={0}
      >
        {tasks.map(({ id, state }, index) => {
          const isUpstreamDone = tasks[index - 1]?.state === "success";

          return (
            <Flex
              alignItems="center"
              as="li"
              color={state === "queued" ? COLORS.muted : COLORS.cream}
              gap={{ base: 2, md: 3 }}
              key={id}
              minWidth={0}
              title={`${id}: ${COPY.states[state]}`}
              transition="color 0.6s ease"
            >
              {index > 0 ? (
                // The edge takes the upstream task's colour once it has succeeded, like the graph view.
                <Box
                  aria-hidden
                  bg={isUpstreamDone ? "success.solid" : COLORS.muted}
                  flexShrink={0}
                  height="2px"
                  opacity={isUpstreamDone ? 0.9 : 0.35}
                  transition="background-color 0.6s ease, opacity 0.6s ease"
                  width={{ base: 4, lg: 16, md: 8 }}
                />
              ) : undefined}
              <Flex
                alignItems="center"
                animation={state === "running" ? `${ring} 1.6s ease-out infinite` : undefined}
                bg={`${state}.solid`}
                borderRadius="full"
                color={`${state}.contrast`}
                flexShrink={0}
                height={{ base: "20px", lg: "32px", md: "26px" }}
                justifyContent="center"
                transition="background-color 0.6s ease"
                width={{ base: "20px", lg: "32px", md: "26px" }}
              >
                <StateIcon size={16} state={state} />
              </Flex>
              {/* On phones only the SmoothOperator task keeps its name. */}
              <Text display={{ base: id === taskId ? "block" : "none", md: "block" }} truncate>
                {id}
              </Text>
              <Text srOnly>{COPY.states[state]}</Text>
            </Flex>
          );
        })}
      </Flex>
    </>
  );
};
