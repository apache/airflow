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
  Flex,
  HStack,
  Heading,
  SimpleGrid,
  Text,
  Tooltip,
  VStack,
  Link,
} from "@chakra-ui/react";
import { FiCalendar } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import type { DAGResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { TogglePause } from "src/components/TogglePause";

import { DagTags } from "./DagTags";

type Props = {
  readonly dag: DAGResponse;
};

export const DagCard = ({ dag }: Props) => (
  <Box
    borderColor="gray.emphasized"
    borderRadius={8}
    borderWidth={1}
    overflow="hidden"
  >
    <Flex
      alignItems="center"
      bg="blue.minimal"
      justifyContent="space-between"
      px={3}
      py={2}
    >
      <HStack>
        <Tooltip hasArrow label={dag.description}>
          <Link
            as={RouterLink}
            color="blue.contrast"
            fontSize="md"
            fontWeight="bold"
            to={`/dags/${dag.dag_id}`}
          >
            {dag.dag_display_name}
          </Link>
        </Tooltip>
        <DagTags tags={dag.tags} />
      </HStack>
      <HStack>
        <TogglePause dagId={dag.dag_id} isPaused={dag.is_paused} />
      </HStack>
    </Flex>
    <SimpleGrid columns={4} height={20} px={3} py={2} spacing={4}>
      <div />
      <VStack align="flex-start" spacing={1}>
        <Heading color="gray.500" fontSize="xs">
          Last Run
        </Heading>
      </VStack>
      <VStack align="flex-start" spacing={1}>
        <Heading color="gray.500" fontSize="xs">
          Next Run
        </Heading>
        <Text fontSize="sm">
          <Time datetime={dag.next_dagrun} />
        </Text>
        {Boolean(dag.timetable_summary) ? (
          <Tooltip hasArrow label={dag.timetable_description}>
            <Text fontSize="sm">
              {" "}
              <FiCalendar style={{ display: "inline" }} />{" "}
              {dag.timetable_summary}
            </Text>
          </Tooltip>
        ) : undefined}
      </VStack>
      <div />
    </SimpleGrid>
  </Box>
);
