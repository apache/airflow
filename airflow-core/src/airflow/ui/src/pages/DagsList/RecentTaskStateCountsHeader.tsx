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
import { Box, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuInfo } from "react-icons/lu";

import { Tooltip } from "src/system-components";

/** Header of the Dags table column, with a tooltip explaining which runs are counted. */
export const RecentTaskStateCountsHeader = () => {
  const { t: translate } = useTranslation("dags");

  return (
    <HStack gap={1}>
      {translate("recentTaskStateCounts.label")}
      <Tooltip content={translate("recentTaskStateCounts.description")} portalled>
        <Box as="span" color="fg.muted" cursor="pointer" data-testid="recent-task-state-counts-info">
          <LuInfo />
        </Box>
      </Tooltip>
    </HStack>
  );
};
