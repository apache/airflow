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
import { Box, HStack, Link, VStack } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import type { DeadlineResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";

type DeadlineItemProps = {
  readonly deadline: DeadlineResponse;
};

const focusStyles = {
  _focus: { outline: "none" },
  _focusVisible: { outline: "2px solid", outlineColor: "brand.focusRing", outlineOffset: "2px" },
};

export const DeadlineItem = ({ deadline }: DeadlineItemProps) => (
  <HStack justify="space-between" px={3} py={2} width="100%">
    <VStack align="start" gap={0} minWidth={0} overflow="hidden">
      <Link asChild color="fg.info" fontSize="sm" fontWeight="medium" {...focusStyles}>
        <RouterLink to={`/dags/${deadline.dag_id}`}>
          <TruncatedText text={deadline.dag_id} />
        </RouterLink>
      </Link>
      <Link asChild color="fg.muted" fontSize="xs" {...focusStyles}>
        <RouterLink to={`/dags/${deadline.dag_id}/runs/${deadline.dag_run_id}`}>
          <TruncatedText text={deadline.dag_run_id} />
        </RouterLink>
      </Link>
    </VStack>
    <Box color="fg.muted" flexShrink={0} fontSize="xs">
      <Time datetime={deadline.deadline_time} />
    </Box>
  </HStack>
);
