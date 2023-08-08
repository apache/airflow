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
  CenterProps,
  Card,
  CardBody,
  CardHeader,
  Center,
  Flex,
  Heading,
  Stack,
  Text,
  FlexProps,
} from "@chakra-ui/react";
import { useHealth } from "src/api";
import type { API } from "src/types";
import Time from "src/components/Time";
import LoadingWrapper from "src/components/LoadingWrapper";

const HealthSection = ({
  title,
  status,
  latestHeartbeat,
  ...rest
}: {
  title: string;
  status?: API.HealthStatus;
  latestHeartbeat?: string | null;
} & FlexProps) => (
  <Flex flexDirection="column" {...rest}>
    <Text as="b" color="blue.600">
      {title}
    </Text>
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
    {latestHeartbeat && (
      <Stack direction="row">
        <Text textIndent="15px" whiteSpace="nowrap">
          last heartbeat:{" "}
        </Text>
        <div>
          <Badge
            colorScheme={status === "healthy" ? "green" : "red"}
            fontSize="0.9rem"
          >
            <Time dateTime={latestHeartbeat} />
          </Badge>
        </div>
      </Stack>
    )}
  </Flex>
);

const Health = (props: CenterProps) => {
  const { data, isError } = useHealth();

  return (
    <Center {...props}>
      <LoadingWrapper hasData={!!data} isError={isError}>
        <Card w="100%">
          <CardHeader textAlign="center" p={3}>
            <Heading size="md">Health</Heading>
          </CardHeader>
          <CardBody>
            <HealthSection
              title="MetaDatabase"
              status={data?.metadatabase?.status}
              mb={3}
            />
            <HealthSection
              title="Scheduler"
              status={data?.scheduler?.status}
              latestHeartbeat={data?.scheduler?.latestSchedulerHeartbeat}
              mb={3}
            />
            <HealthSection
              title="Triggerer"
              status={data?.triggerer?.status}
              latestHeartbeat={data?.triggerer?.latestTriggererHeartbeat}
            />
            <HealthSection
              title="Dag Processor"
              status={data?.dagProcessor?.status}
              latestHeartbeat={data?.dagProcessor?.latestDagProcessorHeartbeat}
            />
          </CardBody>
        </Card>
      </LoadingWrapper>
    </Center>
  );
};

export default Health;
