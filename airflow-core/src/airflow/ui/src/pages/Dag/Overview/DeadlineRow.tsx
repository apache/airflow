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
import { HStack, Link, Text, VStack } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import type { DeadlineResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";

export const DeadlineRow = ({ deadline }: { readonly deadline: DeadlineResponse }) => (
  <HStack justifyContent="space-between" px={2} py={1.5} width="100%">
    <VStack alignItems="flex-start" gap={0}>
      <Link asChild color="fg.info" fontSize="sm" fontWeight="bold">
        <RouterLink to={`/dags/${deadline.dag_id}/runs/${deadline.dag_run_id}`}>
          {deadline.dag_run_id}
        </RouterLink>
      </Link>
      {deadline.alert_name !== undefined && deadline.alert_name !== null && deadline.alert_name !== "" ? (
        <Text color="fg.muted" fontSize="xs">
          {deadline.alert_name}
        </Text>
      ) : undefined}
    </VStack>
    <Time datetime={deadline.deadline_time} fontSize="sm" />
  </HStack>
);
