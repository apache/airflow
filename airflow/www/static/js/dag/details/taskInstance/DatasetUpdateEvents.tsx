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
import React, { useMemo } from "react";
import { Box, Text } from "@chakra-ui/react";

import { useAssetEvents } from "src/api";
import type { DagRun as DagRunType } from "src/types";
import { getMetaValue } from "src/utils";
import { CardDef, CardList } from "src/components/Table";
import type { AssetEvent } from "src/types/api-generated";
import AssetEventCard from "src/components/AssetEventCard";

interface Props {
  runId: DagRunType["runId"];
  taskId: string;
}

const cardDef: CardDef<AssetEvent> = {
  card: ({ row }) => <AssetEventCard assetEvent={row} showSource={false} />,
};

const dagId = getMetaValue("dag_id") || undefined;

const DatasetUpdateEvents = ({ runId, taskId }: Props) => {
  const {
    data: { assetEvents = [] },
    isLoading,
  } = useAssetEvents({
    sourceDagId: dagId,
    sourceRunId: runId,
    sourceTaskId: taskId,
  });

  const columns = useMemo(
    () => [
      {
        Header: "When",
        accessor: "timestamp",
      },
      {
        Header: "Dataset",
        accessor: "assetUri",
      },
      {
        Header: "Triggered Runs",
        accessor: "createdDagruns",
      },
      {
        Header: "Extra",
        accessor: "extra",
      },
    ],
    []
  );

  const data = useMemo(() => assetEvents, [assetEvents]);

  return (
    <Box my={3} flexGrow={1}>
      <Text as="strong" mb={3}>
        Dataset Events
      </Text>
      <Text>Dataset updates caused by this task instance</Text>
      <CardList
        data={data}
        columns={columns}
        isLoading={isLoading}
        cardDef={cardDef}
      />
    </Box>
  );
};

export default DatasetUpdateEvents;
