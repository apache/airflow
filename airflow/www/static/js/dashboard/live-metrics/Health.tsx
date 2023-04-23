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

import React from "react";
import {
  Badge,
  Box,
  Card,
  CardBody,
  CardHeader,
  Flex,
  Heading,
  Spinner,
  Stack,
  Text,
} from "@chakra-ui/react";
import { useHealth } from "src/api";
import type { API } from "src/types";
import Time from "src/components/Time";

const StatusRow = ({ status }: { status?: API.HealthStatus }) => (
  <Stack direction="row">
    <Text textIndent="15px">status:</Text>
    <div>
      <Badge
        colorScheme={status === "healthy" ? "green" : "red"}
        fontSize="0.9rem"
      >
        {status || "unknown"}
      </Badge>
    </div>
  </Stack>
);

const Health = () => {
  const { data, isSuccess } = useHealth();

  return (
    <Box mx={3}>
      {isSuccess ? (
        <Card>
          <CardHeader textAlign="center" p={3}>
            <Heading size="md">Health</Heading>
          </CardHeader>
          <CardBody>
            <Flex flexDirection="column" mb={3}>
              <Text as="b" color="blue.600">
                MetaDatabase
              </Text>
              <StatusRow status={data?.metadatabase?.status} />
            </Flex>
            <Flex flexDirection="column">
              <Text as="b" color="blue.600">
                Scheduler
              </Text>
              <StatusRow status={data?.scheduler?.status} />
              <Stack direction="row">
                <Text textIndent="15px" whiteSpace="nowrap">
                  last heartbeat:{" "}
                </Text>
                <div>
                  <Badge
                    colorScheme={
                      data?.scheduler?.status === "healthy" ? "green" : "red"
                    }
                    fontSize="0.9rem"
                  >
                    <Time
                      dateTime={data?.scheduler?.latestSchedulerHeartbeat}
                    />
                  </Badge>
                </div>
              </Stack>
            </Flex>
          </CardBody>
        </Card>
      ) : (
        <Spinner color="blue.500" speed="1s" mr="4px" size="xl" />
      )}
    </Box>
  );
};

export default Health;
