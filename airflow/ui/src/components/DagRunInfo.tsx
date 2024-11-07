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
import { VStack, Text } from "@chakra-ui/react";
import dayjs from "dayjs";

import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";

type Props = {
  readonly dataIntervalEnd?: string | null;
  readonly dataIntervalStart?: string | null;
  readonly endDate?: string | null;
  readonly logicalDate?: string | null;
  readonly nextDagrunCreateAfter?: string | null;
  readonly startDate?: string | null;
};

const DagRunInfo = ({
  dataIntervalEnd,
  dataIntervalStart,
  endDate,
  logicalDate,
  nextDagrunCreateAfter,
  startDate,
}: Props) =>
  Boolean(dataIntervalStart) && Boolean(dataIntervalEnd) ? (
    <Tooltip
      content={
        <VStack align="left" gap={0}>
          <Text>
            Data Interval Start: <Time datetime={dataIntervalStart} />
          </Text>
          <Text>
            Data Interval End: <Time datetime={dataIntervalEnd} />
          </Text>
          {Boolean(nextDagrunCreateAfter) ? (
            <Text>
              Run After: <Time datetime={nextDagrunCreateAfter} />
            </Text>
          ) : undefined}
          {Boolean(logicalDate) ? (
            <Text>Logical Date: {logicalDate}</Text>
          ) : undefined}
          {Boolean(startDate) ? (
            <Text>Start Date: {startDate}</Text>
          ) : undefined}
          {Boolean(endDate) ? <Text>End Date: {endDate}</Text> : undefined}
          {Boolean(startDate) && Boolean(endDate) ? (
            <Text>
              Duration:{" "}
              {dayjs.duration(dayjs(endDate).diff(startDate)).asSeconds()}s
            </Text>
          ) : undefined}
        </VStack>
      }
      showArrow
    >
      <Text fontSize="sm">
        <Time datetime={dataIntervalStart} showTooltip={false} />
      </Text>
    </Tooltip>
  ) : undefined;

export default DagRunInfo;
