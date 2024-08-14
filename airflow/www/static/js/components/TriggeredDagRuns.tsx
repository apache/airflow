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

import React from "react";

import type { DAGRun } from "src/types/api-generated";
import { Box, Flex, Tooltip, Text, Link } from "@chakra-ui/react";
import { FiLink } from "react-icons/fi";

import { getMetaValue } from "src/utils";
import Time from "src/components/Time";
import { useContainerRef } from "src/context/containerRef";
import { SimpleStatus } from "src/dag/StatusBox";
import { formatDuration, getDuration } from "src/datetime_utils";

type CardProps = {
  createdDagRuns: DAGRun[];
  showLink?: boolean;
};

const gridUrl = getMetaValue("grid_url");
const dagId = getMetaValue("dag_id") || "__DAG_ID__";

const TriggeredDagRuns = ({ createdDagRuns, showLink = true }: CardProps) => {
  const containerRef = useContainerRef();

  return (
    <Flex alignItems="center">
      {createdDagRuns.map((run) => {
        const runId = (run as any).dagRunId; // For some reason the type is wrong here
        const url = `${gridUrl.replace(
          dagId,
          run.dagId || ""
        )}?dag_run_id=${encodeURIComponent(runId)}`;

        return (
          <Tooltip
            key={runId}
            label={
              <Box>
                <Text>DAG Id: {run.dagId}</Text>
                <Text>Status: {run.state || "no status"}</Text>
                <Text>
                  Duration:{" "}
                  {formatDuration(getDuration(run.startDate, run.endDate))}
                </Text>
                <Text>
                  Start Date: <Time dateTime={run.startDate} />
                </Text>
                {run.endDate && (
                  <Text>
                    End Date: <Time dateTime={run.endDate} />
                  </Text>
                )}
              </Box>
            }
            portalProps={{ containerRef }}
            hasArrow
            placement="top"
          >
            <Flex width={showLink ? "30px" : "16px"}>
              <SimpleStatus state={run.state} mx={1} />
              {showLink && (
                <Link color="blue.600" href={url}>
                  <FiLink size="12px" />
                </Link>
              )}
            </Flex>
          </Tooltip>
        );
      })}
    </Flex>
  );
};

export default TriggeredDagRuns;
