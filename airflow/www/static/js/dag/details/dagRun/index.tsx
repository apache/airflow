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
import { Box } from "@chakra-ui/react";

import { useGridData } from "src/api";
import { getMetaValue, useOffsetTop } from "src/utils";
import type { DagRun as DagRunType } from "src/types";
import NotesAccordion from "src/dag/details/NotesAccordion";

import DatasetTriggerEvents from "./DatasetTriggerEvents";
import DagRunDetails from "./Details";

const dagId = getMetaValue("dag_id");

interface Props {
  runId: DagRunType["runId"];
}

const DagRun = ({ runId }: Props) => {
  const {
    data: { dagRuns },
  } = useGridData();
  const detailsRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(detailsRef);

  const run = dagRuns.find((dr) => dr.runId === runId);

  if (!run) return null;
  const { runType, note } = run;

  return (
    <Box
      maxHeight={`calc(100% - ${offsetTop}px)`}
      ref={detailsRef}
      overflowY="auto"
      pb={4}
    >
      <NotesAccordion
        dagId={dagId}
        runId={runId}
        initialValue={note}
        key={dagId + runId}
      />
      {runType === "asset_triggered" && <DatasetTriggerEvents runId={runId} />}
      <DagRunDetails run={run} />
    </Box>
  );
};

export default DagRun;
