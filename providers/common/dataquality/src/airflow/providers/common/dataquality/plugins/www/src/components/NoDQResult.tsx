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

import { Box, Text } from "@chakra-ui/react";
import type { FC } from "react";

export const NoDQResult: FC = () => (
  <Box alignItems="center" bg="bg" color="fg" display="flex" justifyContent="center" minH="200px" p={5}>
    <Box bg="bg.subtle" borderRadius="xl" borderWidth="1px" maxW="440px" p={8} textAlign="center">
      <Text fontSize="3xl" mb={3}>
        &#x1F4CB;
      </Text>
      <Text as="h3" fontSize="md" fontWeight="semibold" mb={2}>
        No data quality results for this task instance
      </Text>
      <Text color="fg.muted" fontSize="sm" lineHeight="tall">
        Either this task doesn't run a <Text as="span" fontWeight="medium">DQCheckOperator</Text>, or it
        hasn't executed yet. Once it runs, results are persisted to the configured{" "}
        <Text as="span" fontWeight="medium">[common.dataquality] results_path</Text> and appear here.
      </Text>
    </Box>
  </Box>
);
