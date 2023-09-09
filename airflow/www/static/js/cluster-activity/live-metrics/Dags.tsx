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
  BoxProps,
  Card,
  CardBody,
  CardHeader,
  Center,
  Flex,
  Heading,
  Text,
} from "@chakra-ui/react";
import { useDags } from "src/api";
import LoadingWrapper from "src/components/LoadingWrapper";

const Dags = (props: BoxProps) => {
  const { data: dataOnlyUnpaused, isError: isErrorUnpaused } = useDags({
    paused: false,
  });

  const { data, isError } = useDags({});

  return (
    <Center {...props}>
      <LoadingWrapper
        hasData={!!data && !!dataOnlyUnpaused}
        isError={isError || isErrorUnpaused}
      >
        <Card w="100%">
          <CardHeader textAlign="center" p={3}>
            <Heading size="md">Unpaused DAGs</Heading>
          </CardHeader>
          <CardBody>
            <Flex justifyContent="center" mb={4}>
              <Heading as="b" size="xl">
                {dataOnlyUnpaused?.totalEntries}
              </Heading>
            </Flex>
            <Flex justifyContent="end" textAlign="right">
              <Text size="md" color="gray.500">
                out of <Text as="b">{data?.totalEntries}</Text> total DAGs
              </Text>
            </Flex>
          </CardBody>
        </Card>
      </LoadingWrapper>
    </Center>
  );
};

export default Dags;
