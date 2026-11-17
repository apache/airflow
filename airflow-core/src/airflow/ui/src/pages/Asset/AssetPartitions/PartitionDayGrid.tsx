import { Fragment } from "react";

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
import { Box, Grid, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Tooltip } from "src/system-components";

import { type MonthOfDays, RUN_STATE_BY_STATUS, STATUS_LABEL_KEY } from "./partitionStatus";

const DAYS = Array.from({ length: 31 }, (_, index) => index + 1);

export const PartitionDayGrid = ({ months }: { readonly months: Array<MonthOfDays> }) => {
  const { t: translate } = useTranslation(["assets", "common"]);

  return (
    <Grid
      alignItems="center"
      data-testid="partition-day-grid"
      gap="2px"
      // Cells shrink with the panel and stop at the size of a Dag calendar cell.
      maxW="560px"
      templateColumns="auto repeat(31, minmax(0, 1fr))"
    >
      {months.map(({ days, month }) => (
        <Fragment key={month}>
          <Text color="fg.muted" fontSize="xs" pe={2} whiteSpace="nowrap">
            {month}
          </Text>
          {DAYS.map((day) => {
            const partition = days.get(day);

            return partition === undefined ? (
              <Box key={day} />
            ) : (
              <Tooltip
                content={`${partition.partition_key}: ${translate(STATUS_LABEL_KEY[partition.status])}`}
                key={day}
              >
                <Box
                  aspectRatio={1}
                  bg={`${RUN_STATE_BY_STATUS[partition.status]}.solid`}
                  borderRadius="2px"
                  data-status={partition.status}
                  data-testid="partition-day-cell"
                />
              </Tooltip>
            );
          })}
        </Fragment>
      ))}
    </Grid>
  );
};
