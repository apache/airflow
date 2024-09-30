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

import React, { useMemo, useState } from "react";
import { snakeCase } from "lodash";
import type { SortingRule } from "react-table";
import { Box, Flex, Heading, Select } from "@chakra-ui/react";

import { useDatasetEvents } from "src/api";

import { CardList, type CardDef } from "src/components/Table";
import type { DatasetEvent } from "src/types/api-generated";
import DatasetEventCard from "src/components/DatasetEventCard";

type Props = {
  assetId?: number;
  showLabel?: boolean;
};

const cardDef: CardDef<DatasetEvent> = {
  card: ({ row }) => <DatasetEventCard datasetEvent={row} />,
};

const Events = ({ assetId, showLabel }: Props) => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([
    { id: "timestamp", desc: true },
  ]);

  const sort = sortBy[0];
  const orderBy = sort ? `${sort.desc ? "-" : ""}${snakeCase(sort.id)}` : "";

  const {
    data: { datasetEvents = [], totalEntries = 0 },
    isLoading: isEventsLoading,
  } = useDatasetEvents({
    assetId,
    limit,
    offset,
    orderBy,
  });

  const columns = useMemo(
    () => [
      {
        Header: "When",
        accessor: "timestamp",
      },
      {
        Header: "Dataset",
        accessor: "datasetUri",
      },
      {
        Header: "Source Task Instance",
        accessor: "sourceTaskId",
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

  const data = useMemo(() => datasetEvents, [datasetEvents]);

  return (
    <Box>
      <Flex justifyContent="space-between" alignItems="center">
        <Heading size="sm">{showLabel && "Events"}</Heading>
        <Flex alignItems="center" alignSelf="flex-end">
          Sort:
          <Select
            ml={2}
            value={orderBy}
            onChange={({ target: { value } }) => {
              const isDesc = value.startsWith("-");
              setSortBy([
                {
                  id: isDesc ? value.slice(0, value.length) : value,
                  desc: isDesc,
                },
              ]);
            }}
            width="200px"
          >
            <option value="-timestamp">Timestamp - Desc</option>
            <option value="timestamp">Timestamp - Asc</option>
          </Select>
        </Flex>
      </Flex>
      <CardList
        data={data}
        columns={columns}
        manualPagination={{
          offset,
          setOffset,
          totalEntries,
        }}
        pageSize={limit}
        isLoading={isEventsLoading}
        cardDef={cardDef}
      />
    </Box>
  );
};

export default Events;
