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

import { Badge, Box, HStack, Heading, Link, Stack, Text } from "@chakra-ui/react";
import type { FC } from "react";

import { Tooltip } from "src/components/Tooltip";
import { describeSkillSource, isLoaded } from "src/skill-source";
import type { SkillInfo } from "src/types/model";

const SkillSourceText: FC<{ skill: SkillInfo }> = ({ skill }) => {
  const { location, path, commit, href } = describeSkillSource(skill.source);
  const text = [location, path, commit].filter((part) => part !== undefined).join(" · ");

  return href === undefined ? (
    <Text color="fg.muted" fontFamily="mono" fontSize="xs" wordBreak="break-all">
      {text}
    </Text>
  ) : (
    <Link color="fg.muted" fontFamily="mono" fontSize="xs" href={href} rel="noreferrer" target="_blank">
      {text}
    </Link>
  );
};

export const SkillsSection: FC<{ skills: SkillInfo[] }> = ({ skills }) => {
  // Loaded skills first: they are what the run actually used.
  const ordered = [...skills].sort((a, b) => Number(isLoaded(b.calls)) - Number(isLoaded(a.calls)));
  const loadedCount = skills.filter((skill) => isLoaded(skill.calls)).length;

  return (
    <Box data-testid="skills-section" maxW="960px" mt={6}>
      <HStack gap={2} mb={3}>
        <Heading size="sm">Skills</Heading>
        <Text color="fg.muted" fontSize="sm">
          {loadedCount} of {skills.length} loaded
        </Text>
      </HStack>
      <Stack gap={2}>
        {ordered.map((skill) => {
          const loaded = isLoaded(skill.calls);
          const calls = Object.entries(skill.calls);

          return (
            <HStack align="center" gap={3} key={skill.name}>
              <Tooltip
                content={
                  <Stack gap={1}>
                    {skill.description ? <Text>{skill.description}</Text> : undefined}
                    <Text>
                      {calls.length === 0
                        ? "Available, not used"
                        : calls.map(([tool, count]) => `${tool} ×${count}`).join(" · ")}
                    </Text>
                  </Stack>
                }
                portalled
              >
                <Badge
                  borderRadius="full"
                  colorPalette={loaded ? "brand" : "gray"}
                  flexShrink={0}
                  fontSize="sm"
                  px={3}
                  py={1}
                  variant={loaded ? "solid" : "outline"}
                >
                  {skill.name}
                </Badge>
              </Tooltip>
              <SkillSourceText skill={skill} />
            </HStack>
          );
        })}
      </Stack>
    </Box>
  );
};
