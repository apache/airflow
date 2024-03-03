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
  Box,
  BoxProps,
  Card,
  CardBody,
  CardHeader,
  Center,
  Flex,
  Heading,
  Link,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
} from "@chakra-ui/react";
import { useDagRuns } from "src/api";
import { formatDuration, getDuration } from "src/datetime_utils";
import LoadingWrapper from "src/components/LoadingWrapper";

const DagRuns = (props: BoxProps) => {
  const { data, isError } = useDagRuns({
    dagId: "~",
    state: ["running"],
    orderBy: "start_date",
    limit: 5,
  });

  return (
    <Center {...props}>
      <LoadingWrapper hasData={!!data} isError={isError}>
        <Card w="100%">
          <CardHeader textAlign="center" p={3}>
            <Heading size="md">Top 5 longest Dag Runs to finish</Heading>
          </CardHeader>
          <CardBody>
            <Box mt={2} mb={5}>
              {data?.totalEntries !== undefined && data.totalEntries > 0 ? (
                <Table
                  size="sm"
                  style={{ tableLayout: "fixed", width: "100%" }}
                >
                  <Thead>
                    <Tr>
                      <Th>Dag Id</Th>
                      <Th>Run Type</Th>
                      <Th>Duration</Th>
                    </Tr>
                  </Thead>
                  <Tbody>
                    {data?.dagRuns?.map((dagRun) => (
                      <Tr key={dagRun.dagRunId}>
                        <Td
                          textOverflow="ellipsis"
                          overflow="hidden"
                          whiteSpace="nowrap"
                        >
                          <Link
                            href={`dags/${
                              dagRun.dagId
                            }/grid?dag_run_id=${encodeURIComponent(
                              dagRun.dagRunId as string
                            )}`}
                          >
                            {dagRun.dagId}
                          </Link>
                        </Td>
                        <Td>{dagRun.runType}</Td>
                        <Td>
                          {formatDuration(
                            getDuration(dagRun.startDate, dagRun.endDate)
                          )}
                        </Td>
                      </Tr>
                    ))}
                  </Tbody>
                </Table>
              ) : (
                <Flex justifyContent="center">
                  <Heading as="b" size="sm">
                    No dag running
                  </Heading>
                </Flex>
              )}
            </Box>
            <Flex justifyContent="end" textAlign="right">
              <Text size="md" color="gray.500">
                out of <Text as="b">{data?.totalEntries}</Text> total running
                Dag Runs
              </Text>
            </Flex>
          </CardBody>
        </Card>
      </LoadingWrapper>
    </Center>
  );
};

export default DagRuns;
