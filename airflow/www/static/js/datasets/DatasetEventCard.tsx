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
import { isEmpty } from "lodash";
import { TbApi } from "react-icons/tb";

import type { DatasetEvent } from "src/types/api-generated";
import { Box, Flex, Tooltip, Text, Grid, GridItem } from "@chakra-ui/react";
import { HiDatabase } from "react-icons/hi";

import Time from "src/components/Time";
import { useContainerRef } from "src/context/containerRef";
import { SimpleStatus } from "src/dag/StatusBox";
import { formatDuration, getDuration } from "src/datetime_utils";

import SourceTaskInstance from "./SourceTaskInstance";
import Extra from "./Extra";

type CardProps = {
  datasetEvent: DatasetEvent;
};

const MAX_RUNS = 9;

const DatasetEventCard = ({ datasetEvent }: CardProps) => {
  const containerRef = useContainerRef();

  const { fromRestApi, ...extra } = datasetEvent?.extra as Record<
    string,
    string
  >;

  return (
    <Grid
      templateRows="repeat(2, 1fr)"
      templateColumns="repeat(4, 1fr)"
      key={`${datasetEvent.datasetId}-${datasetEvent.timestamp}`}
      _hover={{ bg: "gray.50" }}
      transition="background-color 0.2s"
      p={2}
      borderTopWidth={1}
      borderColor="gray.300"
      borderStyle="solid"
    >
      <GridItem rowSpan={2} colSpan={2}>
        <Time dateTime={datasetEvent.timestamp} />
        <Flex alignItems="center">
          <HiDatabase size="16px" />
          <Text ml={2}>{datasetEvent.datasetUri}</Text>
        </Flex>
      </GridItem>
      <GridItem>
        Source:
        {fromRestApi && (
          <Tooltip
            portalProps={{ containerRef }}
            hasArrow
            placement="top"
            label="Manually created from REST API"
          >
            <Box width="20px">
              <TbApi size="20px" />
            </Box>
          </Tooltip>
        )}
        {!!datasetEvent.sourceTaskId && (
          <SourceTaskInstance datasetEvent={datasetEvent} />
        )}
      </GridItem>
      <GridItem>
        {!!datasetEvent?.createdDagruns?.length && (
          <>
            Triggered Dag Runs:
            <Flex alignItems="center">
              {(datasetEvent?.createdDagruns || [])
                ?.slice(0, MAX_RUNS)
                .map((run) => (
                  <Tooltip
                    key={(run as any).dagRunId} // For some reason the type is wrong here
                    label={
                      <Box>
                        <Text>DAG Id: {run.dagId}</Text>
                        <Text>Status: {run.state || "no status"}</Text>
                        <Text>
                          Duration:{" "}
                          {formatDuration(
                            getDuration(run.startDate, run.endDate)
                          )}
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
                    <span>
                      <SimpleStatus state={run.state} mx={1} />
                    </span>
                  </Tooltip>
                ))}
              {datasetEvent?.createdDagruns.length > MAX_RUNS && (
                <Text>
                  +{(datasetEvent.createdDagruns.length || 0) - MAX_RUNS}
                </Text>
              )}
            </Flex>
          </>
        )}
      </GridItem>
      {!isEmpty(extra) && (
        <GridItem colSpan={2}>
          <Extra extra={extra} />
        </GridItem>
      )}
    </Grid>
  );
};

export default DatasetEventCard;
