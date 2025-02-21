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
import { MdOutlineAccountTree } from "react-icons/md";
import { Link } from "react-router-dom";

import type { AssetEventResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";

export const AssetEvent = ({ event }: { readonly event: AssetEventResponse }) => {
  const hasDagRuns = event.created_dagruns.length > 0;
  let source = "";

  if (event.extra?.from_rest_api === true) {
    source = "API";
  } else if (event.extra?.from_trigger === true) {
    source = "Trigger";
  }

  return (
    <Box fontSize={13} mt={1} w="full">
      <Text fontWeight="bold">
        <Time datetime={event.timestamp} />
      </Text>
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
          <Text> {event.name ?? ""} </Text>
        </Tooltip>
      </HStack>
      <HStack>
        <MdOutlineAccountTree /> <Text> Source: </Text>
        {source === "" ? (
          <Link
            to={`/dags/${event.source_dag_id}/runs/${event.source_run_id}/tasks/${event.source_task_id}?map_index=${event.source_map_index}`}
          >
            <Text color="fg.info"> {event.source_dag_id} </Text>
          </Link>
        ) : (
          source
        )}
      </HStack>
      <HStack>
        <Text> Triggered Dag Runs: </Text>
        {hasDagRuns ? (
          <Link to={`/dags/${event.created_dagruns[0]?.dag_id}/runs/${event.created_dagruns[0]?.run_id}`}>
            <Text color="fg.info"> {event.created_dagruns[0]?.dag_id} </Text>
          </Link>
        ) : (
          "~"
        )}
      </HStack>
    </Box>
  );
};
