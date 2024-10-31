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
import { Tooltip, VStack, Text } from "@chakra-ui/react";

import type { DAGResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";

type Props = {
  readonly dag: DAGResponse;
};

const DagRunInfo = ({ dag }: Props) => (
  <Tooltip
    hasArrow
    label={
      <VStack align="left" gap={0}>
        <Text>Data Interval</Text>
        <Text>
          Start: <Time datetime={dag.next_dagrun_data_interval_start} />
        </Text>
        <Text>
          End: <Time datetime={dag.next_dagrun_data_interval_end} />
        </Text>
        <Text>
          Run After: <Time datetime={dag.next_dagrun_create_after} />
        </Text>
      </VStack>
    }
  >
    <Text fontSize="sm">
      <Time datetime={dag.next_dagrun} showTooltip={false} />
    </Text>
  </Tooltip>
);

export default DagRunInfo;
