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
import React, { useRef } from "react";
import {
  Flex,
  Box,
  Button,
  Divider,
  Spacer,
  Table,
  Tbody,
  Tr,
  Td,
  useClipboard,
} from "@chakra-ui/react";

import ReactJson from "react-json-view";

import { useGridData } from "src/api";
import { getMetaValue, useOffsetTop } from "src/utils";
import type { DagRun as DagRunType } from "src/types";
import { SimpleStatus } from "src/dag/StatusBox";
import { ClipboardText } from "src/components/Clipboard";
import { formatDuration, getDuration } from "src/datetime_utils";
import Time from "src/components/Time";
import RunTypeIcon from "src/components/RunTypeIcon";
import NotesAccordion from "src/dag/details/NotesAccordion";

import DatasetTriggerEvents from "./DatasetTriggerEvents";

const dagId = getMetaValue("dag_id");

interface Props {
  runId: DagRunType["runId"];
}

const formatConf = (conf: string | null | undefined): string => {
  if (!conf) {
    return "";
  }
  return JSON.stringify(JSON.parse(conf), null, 4);
};

const DagRun = ({ runId }: Props) => {
  const {
    data: { dagRuns },
  } = useGridData();
  const detailsRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(detailsRef);

  const run = dagRuns.find((dr) => dr.runId === runId);
  const { onCopy, hasCopied } = useClipboard(formatConf(run?.conf));
  if (!run) return null;
  const {
    state,
    runType,
    lastSchedulingDecision,
    dataIntervalStart,
    dataIntervalEnd,
    startDate,
    endDate,
    queuedAt,
    externalTrigger,
    conf,
    confIsJson,
    note,
  } = run;

  return (
    <Box
      maxHeight={`calc(100% - ${offsetTop}px)`}
      ref={detailsRef}
      overflowY="auto"
      pb={4}
    >
      <Box px={4}>
        <NotesAccordion
          dagId={dagId}
          runId={runId}
          initialValue={note}
          key={dagId + runId}
        />
      </Box>
      <Divider my={0} />
      <Table variant="striped">
        <Tbody>
          <Tr>
            <Td>Status</Td>
            <Td>
              <Flex>
                <SimpleStatus state={state} mx={2} />
                {state || "no status"}
              </Flex>
            </Td>
          </Tr>
          <Tr>
            <Td>Run ID</Td>
            <Td>
              <ClipboardText value={runId} />
            </Td>
          </Tr>
          <Tr>
            <Td>Run type</Td>
            <Td>
              <RunTypeIcon runType={runType} />
              {runType}
            </Td>
          </Tr>
          {startDate && (
            <Tr>
              <Td>Run duration</Td>
              <Td>{formatDuration(getDuration(startDate, endDate))}</Td>
            </Tr>
          )}
          {lastSchedulingDecision && (
            <Tr>
              <Td>Last scheduling decision</Td>
              <Td>
                <Time dateTime={lastSchedulingDecision} />
              </Td>
            </Tr>
          )}
          {queuedAt && (
            <Tr>
              <Td>Queued at</Td>
              <Td>
                <Time dateTime={queuedAt} />
              </Td>
            </Tr>
          )}
          {startDate && (
            <Tr>
              <Td>Started</Td>
              <Td>
                <Time dateTime={startDate} />
              </Td>
            </Tr>
          )}
          {endDate && (
            <Tr>
              <Td>Ended</Td>
              <Td>
                <Time dateTime={endDate} />
              </Td>
            </Tr>
          )}
          {dataIntervalStart && dataIntervalEnd && (
            <>
              <Tr>
                <Td>Data interval start</Td>
                <Td>
                  <Time dateTime={dataIntervalStart} />
                </Td>
              </Tr>
              <Tr>
                <Td>Data interval end</Td>
                <Td>
                  <Time dateTime={dataIntervalEnd} />
                </Td>
              </Tr>
            </>
          )}
          <Tr>
            <Td>Externally triggered</Td>
            <Td>{externalTrigger ? "True" : "False"}</Td>
          </Tr>
          <Tr>
            <Td>Run config</Td>
            {confIsJson ? (
              <Td>
                <Flex>
                  <ReactJson
                    src={JSON.parse(conf ?? "")}
                    name={false}
                    theme="rjv-default"
                    iconStyle="triangle"
                    indentWidth={2}
                    displayDataTypes={false}
                    enableClipboard={false}
                    style={{ backgroundColor: "inherit" }}
                  />
                  <Spacer />
                  <Button aria-label="Copy" onClick={onCopy}>
                    {hasCopied ? "Copied!" : "Copy"}
                  </Button>
                </Flex>
              </Td>
            ) : (
              <Td>{conf ?? "None"}</Td>
            )}
          </Tr>
        </Tbody>
      </Table>
      {runType === "dataset_triggered" && (
        <DatasetTriggerEvents runId={runId} />
      )}
    </Box>
  );
};

export default DagRun;
