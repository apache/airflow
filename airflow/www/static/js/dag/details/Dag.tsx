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

import React, { useRef, ReactNode, useState, useEffect } from "react";
import {
  Table,
  Tbody,
  Tr,
  Td,
  Link,
  Button,
  Flex,
  Heading,
  Text,
  Box,
  Center,
  Badge,
  Code,
} from "@chakra-ui/react";
import { mean, omit } from "lodash";

import { getDuration, formatDuration } from "src/datetime_utils";
import {
  appendSearchParams,
  finalStatesMap,
  getMetaValue,
  getTaskSummary,
  toSentenceCase,
  useOffsetTop,
} from "src/utils";
import { useGridData, useDag, useDagDetails } from "src/api";
import Time from "src/components/Time";
import ViewScheduleInterval from "src/components/ViewScheduleInterval";
import type { TaskState } from "src/types";

import type { DAG, DAGDetail } from "src/types/api-generated";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";
import LinkButton from "src/components/LinkButton";
import { SimpleStatus } from "../StatusBox";

const dagId = getMetaValue("dag_id");
const tagIndexUrl = getMetaValue("tag_index_url");
const taskInstancesUrl = getMetaValue("task_instances_list_url");

const Dag = () => {
  const {
    data: { dagRuns, groups },
  } = useGridData();
  const detailsRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(detailsRef);

  const { data: dagData, isLoading: isLoadingDag } = useDag();
  const {
    data: dagDetailsData,
    isLoading: isLoadingDagDetails,
    refetch: refetchDagDetails,
  } = useDagDetails();
  const [excludeFromDagDetails, setExcludeFromDagDetials] = useState<
    Array<string>
  >([]);

  const listParams = new URLSearchParamsWrapper({
    _flt_3_dag_id: dagId,
  });

  const getRedirectUri = (state: string): string => {
    listParams.set("_flt_3_state", state);
    return appendSearchParams(taskInstancesUrl, listParams);
  };

  const taskSummary = getTaskSummary({ task: groups });
  const numMap = finalStatesMap();
  const durations: number[] = [];

  const dagDataExcludeFields = [
    "defaultView",
    "fileToken",
    "scheduleInterval",
    "tags",
    "owners",
    "params",
  ];

  const fetchDagDetails = () => {
    if (dagData) {
      setExcludeFromDagDetials([
        ...Object.keys(dagData),
        ...dagDataExcludeFields,
      ]);
    }
  };

  // ensures dag/{dag_id}/details is called only after data
  // is excluded which is common in dags/{dag_id} and dag/{dag_id}/details
  useEffect(() => {
    if (excludeFromDagDetails.length > 0) {
      refetchDagDetails();
    }
  }, [excludeFromDagDetails, refetchDagDetails]);

  dagRuns.forEach((dagRun) => {
    durations.push(getDuration(dagRun.startDate, dagRun.endDate));
    const stateKey = dagRun.state == null ? "no_status" : dagRun.state;
    if (numMap.has(stateKey))
      numMap.set(stateKey, (numMap.get(stateKey) || 0) + 1);
  });

  const stateSummary: ReactNode[] = [];
  numMap.forEach((key, val) => {
    if (key > 0) {
      stateSummary.push(
        // eslint-disable-next-line react/no-array-index-key
        <Tr key={val}>
          <Td>
            <Flex alignItems="center">
              <SimpleStatus state={val as TaskState} />
              <LinkButton
                href={getRedirectUri(val)}
                title={`View all ${val} DAGS`}
              >
                Total {val}
              </LinkButton>
            </Flex>
          </Td>
          <Td>{key}</Td>
        </Tr>
      );
    }
  });

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(null, durations);
  const min = Math.min.apply(null, durations);
  const avg = mean(durations);
  const firstStart = dagRuns[0]?.startDate;
  const lastStart = dagRuns[dagRuns.length - 1]?.startDate;

  // parse value for each key (string | null) and
  const parseDagData = (value: string) => (
    <>
      {value !== "null" &&
        // converting value to string since it could be bool or number
        (Number.isNaN(Date.parse(String(value))) ? (
          String(value)
        ) : (
          <Time dateTime={String(value)} />
        ))}
      {value === "null" && "None"}
    </>
  );

  // render dag and dag_details data
  const renderDagDetailsData = (
    data: DAG | DAGDetail,
    excludekeys: Array<string>
  ) => (
    <>
      {Object.entries(data).map(
        ([key, value]) =>
          !excludekeys.includes(key) && (
            <Tr key={key}>
              <Td>{toSentenceCase(key)}</Td>
              <Td>{parseDagData(String(value))}</Td>
            </Tr>
          )
      )}
    </>
  );

  return (
    <Box
      height="100%"
      maxHeight={`calc(100% - ${offsetTop}px)`}
      ref={detailsRef}
      overflowY="auto"
    >
      <Table variant="striped">
        <Tbody>
          {durations.length > 0 && (
            <>
              <Tr borderBottomWidth={2} borderBottomColor="gray.300">
                <Td>
                  <Heading size="sm">DAG Runs Summary</Heading>
                </Td>
                <Td />
              </Tr>
              <Tr>
                <Td>Total Runs Displayed</Td>
                <Td>{durations.length}</Td>
              </Tr>
              {stateSummary}
              {firstStart && (
                <Tr>
                  <Td>First Run Start</Td>
                  <Td>
                    <Time dateTime={firstStart} />
                  </Td>
                </Tr>
              )}
              {lastStart && (
                <Tr>
                  <Td>Last Run Start</Td>
                  <Td>
                    <Time dateTime={lastStart} />
                  </Td>
                </Tr>
              )}
              <Tr>
                <Td>Max Run Duration</Td>
                <Td>{formatDuration(max)}</Td>
              </Tr>
              <Tr>
                <Td>Mean Run Duration</Td>
                <Td>{formatDuration(avg)}</Td>
              </Tr>
              <Tr>
                <Td>Min Run Duration</Td>
                <Td>{formatDuration(min)}</Td>
              </Tr>
            </>
          )}
          <Tr borderBottomWidth={2} borderBottomColor="gray.300">
            <Td>
              <Heading size="sm">DAG Summary</Heading>
            </Td>
            <Td />
          </Tr>
          <Tr>
            <Td>Total Tasks</Td>
            <Td>{taskSummary.taskCount}</Td>
          </Tr>
          {!!taskSummary.groupCount && (
            <Tr>
              <Td>Total Task Groups</Td>
              <Td>{taskSummary.groupCount}</Td>
            </Tr>
          )}
          {Object.entries(taskSummary.operators).map(([key, value]) => (
            <Tr key={key}>
              <Td>
                {key}
                {value > 1 && "s"}
              </Td>
              <Td>{value}</Td>
            </Tr>
          ))}
          {!isLoadingDag && !!dagData && (
            <>
              <Tr borderBottomWidth={2} borderBottomColor="gray.300">
                <Td>
                  <Heading size="sm">DAG Details</Heading>
                </Td>
                <Td />
              </Tr>
              {renderDagDetailsData(dagData, dagDataExcludeFields)}
              <Tr>
                <Td>Owners</Td>
                <Td>
                  <Flex flexWrap="wrap">
                    {dagData.owners?.map((owner) => (
                      <Badge key={owner} colorScheme="blue">
                        {owner}
                      </Badge>
                    ))}
                  </Flex>
                </Td>
              </Tr>
              <Tr>
                <Td>Tags</Td>
                <Td>
                  {!!dagData.tags && dagData.tags?.length > 0 ? (
                    <Flex flexWrap="wrap">
                      {dagData.tags?.map((tag) => (
                        <Button
                          key={tag.name}
                          as={Link}
                          colorScheme="teal"
                          size="xs"
                          href={`${tagIndexUrl}${tag.name}`}
                          mr={3}
                        >
                          {tag.name}
                        </Button>
                      ))}
                    </Flex>
                  ) : (
                    "No tags"
                  )}
                </Td>
              </Tr>
              <Tr>
                <Td>Schedule interval</Td>
                <Td>
                  {dagData.scheduleInterval?.type === "CronExpression" ? (
                    <Text>{dagData.scheduleInterval?.value}</Text>
                  ) : (
                    // for TimeDelta and RelativeDelta
                    <ViewScheduleInterval
                      data={omit(dagData.scheduleInterval, ["type", "value"])}
                    />
                  )}
                </Td>
              </Tr>
              {!isLoadingDagDetails && !!dagDetailsData && (
                <>
                  {renderDagDetailsData(dagDetailsData, excludeFromDagDetails)}
                  <Tr>
                    <Td>Params</Td>
                    <Td>
                      <Code width="100%">
                        <pre>
                          {JSON.stringify(dagDetailsData.params, null, 2)}
                        </pre>
                      </Code>
                    </Td>
                  </Tr>
                </>
              )}
            </>
          )}
        </Tbody>
      </Table>
      {!isLoadingDag && !dagDetailsData && (
        <Center>
          <Button
            isLoading={isLoadingDagDetails}
            loadingText="Loading..."
            colorScheme="teal"
            variant="outline"
            margin={5}
            mt={5}
            onClick={() => fetchDagDetails()}
          >
            More Details
          </Button>
        </Center>
      )}
    </Box>
  );
};

export default Dag;
