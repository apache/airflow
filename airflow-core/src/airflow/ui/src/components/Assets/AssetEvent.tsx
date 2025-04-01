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
import { Box, Text, HStack, Code } from "@chakra-ui/react";
import { FiDatabase } from "react-icons/fi";
import { Link } from "react-router-dom";

import type { AssetEventResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";

import { TriggeredRuns } from "./TriggeredRuns";

export const AssetEvent = ({
  assetId,
  event,
  showExtra,
}: {
  readonly assetId?: number;
  readonly event: AssetEventResponse;
  readonly showExtra?: boolean;
}) => {
  let source = "";

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const { from_rest_api, from_trigger, ...extra } = event.extra ?? {};

  if (from_rest_api === true) {
    source = "API";
  } else if (from_trigger === true) {
    source = "Trigger";
  }

  const extraString = JSON.stringify(extra);

  return (
    <Box borderBottomWidth={1} fontSize={13} mt={1} p={2}>
      <Text fontWeight="bold">
        <Time datetime={event.timestamp} />
      </Text>
      {Boolean(assetId) ? undefined : (
        <HStack>
          <FiDatabase />
          <Tooltip
            content={
              <div>
                <Text> group: {event.group ?? ""} </Text>
                <Text> uri: {event.uri ?? ""} </Text>
              </div>
            }
            showArrow
          >
            <Link to={`/assets/${event.asset_id}`}>
              <Text color="fg.info"> {event.name ?? ""} </Text>
            </Link>
          </Tooltip>
        </HStack>
      )}
      <HStack>
        <Text>Source: </Text>
        {source === "" ? (
          <Link
            to={`/dags/${event.source_dag_id}/runs/${event.source_run_id}/tasks/${event.source_task_id}${event.source_map_index > -1 ? `/mapped/${event.source_map_index}` : ""}`}
          >
            <Text color="fg.info"> {event.source_dag_id} </Text>
          </Link>
        ) : (
          source
        )}
      </HStack>
      <HStack>
        <TriggeredRuns dagRuns={event.created_dagruns} />
      </HStack>
      {showExtra && extraString !== "{}" ? <Code>{extraString}</Code> : undefined}
    </Box>
  );
};
