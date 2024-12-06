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
import { Box, Text, Button } from "@chakra-ui/react";
import { FiChevronRight } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import { MetricsBadge } from "src/components/MetricsBadge";
import { capitalize } from "src/utils";

// TODO: Add badge count once API is available

export const DagFilterButton = ({
  badgeColor,
  filter,
  link,
}: {
  readonly badgeColor: string;
  readonly filter: string;
  readonly link: string;
}) => (
  <RouterLink to={link}>
    <Button
      alignItems="center"
      borderRadius="md"
      display="flex"
      gap={2}
      variant="outline"
    >
      <Box alignItems="center" display="flex" gap={1}>
        <MetricsBadge backgroundColor={badgeColor} />
        <Text fontWeight="bold">{capitalize(filter)} Dags</Text>
        <FiChevronRight />
      </Box>
    </Button>
  </RouterLink>
);
