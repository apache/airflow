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
import { Box, Text, HStack } from "@chakra-ui/react";
import { FiDatabase } from "react-icons/fi";
import { Link } from "react-router-dom";

import type { AssetEventResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";

import RenderedJsonField from "../RenderedJsonField";
import { TriggeredRuns } from "./TriggeredRuns";

export const AssetEvent = ({
  assetId,
  event,
}: {
  readonly assetId?: number;
  readonly event: AssetEventResponse;
}) => {
  let source = "";

  const { from_rest_api: fromRestAPI, from_trigger: fromTrigger, ...extra } = event.extra ?? {};

  if (fromRestAPI === true) {
    source = "API";
  } else if (fromTrigger === true) {
    source = "Trigger";
  }

  return (
    <Box borderBottomWidth={1} fontSize={13} pb={2}>
      <Text fontWeight="bold">
        <Time datetime={event.timestamp} />
      </Text>
      {Boolean(assetId) ? undefined : (
        <HStack>
          <Box>
            <FiDatabase />
          </Box>
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
              <Box color="fg.info" overflowWrap="anywhere" padding={0} wordWrap="break-word">
                {event.name ?? ""}
              </Box>
            </Link>
          </Tooltip>
        </HStack>
      )}
      <HStack>
        <Box>Source: </Box>
        {source === "" ? (
          <Link
            to={`/dags/${event.source_dag_id}/runs/${event.source_run_id}/tasks/${event.source_task_id}${event.source_map_index > -1 ? `/mapped/${event.source_map_index}` : ""}`}
          >
            <Box color="fg.info" overflowWrap="anywhere" padding={0} wordWrap="break-word">
              {event.source_dag_id}
            </Box>
          </Link>
        ) : (
          source
        )}
      </HStack>
      <HStack>
        <TriggeredRuns dagRuns={event.created_dagruns} />
      </HStack>
      {Object.keys(extra).length >= 1 ? (
        <RenderedJsonField content={extra} jsonProps={{ collapsed: true }} />
      ) : undefined}
    </Box>
  );
};
