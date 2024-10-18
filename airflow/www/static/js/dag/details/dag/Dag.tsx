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

import React, { useRef, ReactNode } from "react";
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
import { useGridData, useDagDetails } from "src/api";
import Time from "src/components/Time";
import ViewTimeDelta from "src/components/ViewTimeDelta";
import type { TaskState } from "src/types";

import type { DAG, DAGDetail } from "src/types/api-generated";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";
import { SimpleStatus } from "../../StatusBox";

const dagId = getMetaValue("dag_id");
const tagIndexUrl = getMetaValue("tag_index_url");
const taskInstancesUrl = getMetaValue("task_instances_list_url");

const Dag = () => {
  const {
    data: { dagRuns, groups },
  } = useGridData();
  const detailsRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(detailsRef);

  const { data: dagDetailsData, isLoading: isLoadingDagDetails } =
    useDagDetails();

  // fields to exclude from "dagDetailsData" since handled separately or not required
  const dagDataExcludeFields = [
    "defaultView",
    "fileToken",
    "timetableSummary",
    "tags",
    "owners",
    "params",
    "dagRunTimeout",
  ];

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
              <Link
                href={getRedirectUri(val)}
                title={`View all ${val} DAGS`}
                color="blue"
                ml="5px"
              >
                Total {val}
              </Link>
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

  // parse value for each key if date or not
  // check "!Number.isNaN(value)" is needed due to
  // Date.parse() sometimes returning valid date's timestamp for numbers.
  const parseStringData = (value: string) =>
    Number.isNaN(Date.parse(value)) || !Number.isNaN(value) ? (
      value
    ) : (
      <Time dateTime={value} />
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
              <Td>{parseStringData(String(value))}</Td>
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
          {!isLoadingDagDetails && !!dagDetailsData && (
            <>
              <Tr borderBottomWidth={2} borderBottomColor="gray.300">
                <Td>
                  <Heading size="sm">DAG Details</Heading>
                </Td>
                <Td />
              </Tr>
              {!!dagDetailsData.assetExpression && (
                <Tr>
                  <Td>Asset Conditions</Td>
                  <Td>
                    <Code>
                      <pre>
                        {JSON.stringify(
                          dagDetailsData.assetExpression,
                          null,
                          2
                        )}
                      </pre>
                    </Code>
                  </Td>
                </Tr>
              )}
              {renderDagDetailsData(dagDetailsData, dagDataExcludeFields)}
              <Tr>
                <Td>Owners</Td>
                <Td>
                  <Flex flexWrap="wrap">
                    {dagDetailsData.owners?.map((owner) => (
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
                  {!!dagDetailsData.tags && dagDetailsData.tags?.length > 0 ? (
                    <Flex flexWrap="wrap">
                      {dagDetailsData.tags?.map((tag) => (
                        <Button
                          key={tag.name}
                          as={Link}
                          colorScheme="teal"
                          size="xs"
                          href={tagIndexUrl.replace(
                            "_TAG_NAME_",
                            tag?.name || ""
                          )}
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
                <Td>Timetable</Td>
                <Td>
                  <Text>{dagDetailsData.timetableSummary || ""}</Text>
                </Td>
              </Tr>
              <Tr>
                <Td>Dag run timeout</Td>
                <Td>
                  {dagDetailsData.dagRunTimeout?.type === undefined ? (
                    <Text>null</Text>
                  ) : (
                    // for TimeDelta and RelativeDelta
                    <ViewTimeDelta
                      data={omit(dagDetailsData.dagRunTimeout, ["type"])}
                    />
                  )}
                </Td>
              </Tr>
              <Tr>
                <Td>Params</Td>
                <Td>
                  <Code width="100%">
                    <pre>{JSON.stringify(dagDetailsData.params, null, 2)}</pre>
                  </Code>
                </Td>
              </Tr>
            </>
          )}
        </Tbody>
      </Table>
    </Box>
  );
};

export default Dag;
