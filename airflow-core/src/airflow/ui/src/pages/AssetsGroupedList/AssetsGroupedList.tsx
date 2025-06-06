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
import { Box, Link, IconButton, VStack, Flex, Heading, Text, Input } from "@chakra-ui/react";
import { useState, useMemo } from "react";
import { Link as RouterLink } from "react-router-dom";

import { useAssetServiceGetAssets } from "openapi/queries";
import type { AssetResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

import { CreateAssetEvent } from "../Asset/CreateAssetEvent";
import { DependencyPopover } from "../AssetsList/DependencyPopover";

// Ícones SVG inline para expandir/colapsar
const ChevronDownIcon = () => (
  <svg fill="currentColor" height="1em" viewBox="0 0 20 20" width="1em">
    <path
      clipRule="evenodd"
      d="M5.23 7.21a.75.75 0 011.06.02L10 11.085l3.71-3.855a.75.75 0 111.08 1.04l-4.24 4.4a.75.75 0 01-1.08 0l-4.24-4.4a.75.75 0 01.02-1.06z"
      fillRule="evenodd"
    />
  </svg>
);

const ChevronRightIcon = () => (
  <svg fill="currentColor" height="1em" viewBox="0 0 20 20" width="1em">
    <path
      clipRule="evenodd"
      d="M7.21 5.23a.75.75 0 011.06-.02l4.4 4.24a.75.75 0 010 1.08l-4.4 4.24a.75.75 0 11-1.04-1.08L11.085 10 7.23 6.29a.75.75 0 01-.02-1.06z"
      fillRule="evenodd"
    />
  </svg>
);

const NameCell = ({ original }: { readonly original: AssetResponse }) => (
  <Link asChild color="fg.info" fontWeight="bold">
    <RouterLink to={`/assets/${original.id}`}>{original.name}</RouterLink>
  </Link>
);

const LastAssetEventCell = ({ original }: { readonly original: AssetResponse }) => {
  const assetEvent = original.last_asset_event;
  const timestamp = assetEvent?.timestamp;

  if (timestamp === undefined || timestamp === null) {
    return undefined;
  }

  return <Time datetime={timestamp} />;
};

const GroupCell = ({ original }: { readonly original: AssetResponse }) => {
  const { group } = original;

  if (!group) {
    return undefined;
  }

  return (
    <Link asChild color="fg.info" fontWeight="bold">
      <RouterLink to={`/assets/group/${group}`}>{group}</RouterLink>
    </Link>
  );
};

const ConsumingDagsCell = ({ original }: { readonly original: AssetResponse }) =>
  original.consuming_dags.length ? (
    <DependencyPopover dependencies={original.consuming_dags} type="Dag" />
  ) : undefined;

const ProducingTasksCell = ({ original }: { readonly original: AssetResponse }) =>
  original.producing_tasks.length ? (
    <DependencyPopover dependencies={original.producing_tasks} type="Task" />
  ) : undefined;

const TriggerCell = ({ original }: { readonly original: AssetResponse }) => (
  <CreateAssetEvent asset={original} withText={false} />
);

const truncate = (str: string, max = 32) => (str.length > max ? `${str.slice(0, max - 3)}...` : str);

const nameCellRenderer = ({ row: { original } }: { row: { original: AssetResponse } }) => (
  <NameCell original={original} />
);
const lastAssetEventCellRenderer = ({ row: { original } }: { row: { original: AssetResponse } }) => (
  <LastAssetEventCell original={original} />
);
const groupCellRenderer = ({ row: { original } }: { row: { original: AssetResponse } }) => (
  <GroupCell original={original} />
);
const consumingDagsCellRenderer = ({ row: { original } }: { row: { original: AssetResponse } }) => (
  <ConsumingDagsCell original={original} />
);
const producingTasksCellRenderer = ({ row: { original } }: { row: { original: AssetResponse } }) => (
  <ProducingTasksCell original={original} />
);
const triggerCellRenderer = ({ row: { original } }: { row: { original: AssetResponse } }) => (
  <TriggerCell original={original} />
);

export const AssetsGroupedList = () => {
  const { setTableURLState, tableURLState } = useTableURLState();
  const { sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const { data, error, isLoading } = useAssetServiceGetAssets({
    limit: 1000,
    offset: 0,
    orderBy,
  });

  const [search, setSearch] = useState("");
  const [sortAsc, setSortAsc] = useState(true);

  // Group assets by group
  const grouped = useMemo(() => {
    const filtered = (data?.assets ?? []).filter((asset) =>
      asset.group.toLowerCase().includes(search.toLowerCase()),
    );
    const groupedObj = filtered.reduce<Record<string, Array<AssetResponse>>>((acc, asset) => {
      const groupName = asset.group;

      acc[groupName] ??= [];
      acc[groupName].push(asset);

      return acc;
    }, {});

    // Sort groups by name
    const sortedGroups = Object.entries(groupedObj).sort(([groupA], [groupB]) =>
      sortAsc ? groupA.localeCompare(groupB) : groupB.localeCompare(groupA),
    );

    return sortedGroups.map(([group, assets]) => ({
      assets,
      count: assets.length,
      group,
    }));
  }, [data, search, sortAsc]);

  const columns = [
    {
      accessorKey: "name",
      cell: nameCellRenderer,
      header: () => "Name",
      size: 300,
    },
    {
      accessorKey: "last_asset_event",
      cell: lastAssetEventCellRenderer,
      enableSorting: false,
      header: () => "Last Asset Event",
      size: 200,
    },
    {
      accessorKey: "group",
      cell: groupCellRenderer,
      enableSorting: false,
      header: () => "Group",
      size: 200,
    },
    {
      accessorKey: "consuming_dags",
      cell: consumingDagsCellRenderer,
      enableSorting: false,
      header: () => "Consuming Dags",
      size: 200,
    },
    {
      accessorKey: "producing_tasks",
      cell: producingTasksCellRenderer,
      enableSorting: false,
      header: () => "Producing Tasks",
      size: 200,
    },
    {
      accessorKey: "trigger",
      cell: triggerCellRenderer,
      enableSorting: false,
      header: "",
      size: 80,
    },
  ];

  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({});

  const handleToggleGroup = (groupName: string) => {
    setExpandedGroups((prev) => ({
      ...prev,
      [groupName]: !prev[groupName],
    }));
  };

  return (
    <VStack align="stretch" gap={2} width="100%">
      {/* Searchbar and sort button */}
      <Box alignItems="center" display="flex" gap={2} mb={2}>
        <Input
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search group"
          size="sm"
          value={search}
          width="250px"
        />
        <IconButton
          aria-label="Sort groups"
          onClick={() => setSortAsc((sortAscValue) => !sortAscValue)}
          size="sm"
          variant="outline"
        >
          {sortAsc ? <ChevronDownIcon /> : <ChevronRightIcon />}
        </IconButton>
      </Box>
      {grouped.map(({ assets, count, group }) => (
        <Box borderRadius="md" borderWidth={1} key={group} overflow="hidden" width="100%">
          <Flex
            _hover={{ bg: "chakra-subtle-bg-hover" }}
            align="center"
            bg="chakra-subtle-bg"
            cursor="pointer"
            onClick={() => handleToggleGroup(group)}
            pb={2}
            pt={4}
            px={4}
            userSelect="none"
          >
            <IconButton
              aria-label={expandedGroups[group] ? "Collapse group" : "Expand group"}
              mr={2}
              onClick={(event) => {
                event.stopPropagation();
                handleToggleGroup(group);
              }}
              size="sm"
              variant="ghost"
            >
              {expandedGroups[group] ? <ChevronDownIcon /> : <ChevronRightIcon />}
            </IconButton>
            <Heading
              alignItems="center"
              display="flex"
              flex="1"
              maxW="80%"
              mb={0}
              overflow="hidden"
              size="sm"
              textOverflow="ellipsis"
              title={group}
              whiteSpace="nowrap"
            >
              <Link asChild color="fg.info" fontWeight="bold">
                <RouterLink to={`/assets/group/${encodeURIComponent(group)}`}>{truncate(group)}</RouterLink>
              </Link>
              <Text color="gray.400" flexShrink={0} fontSize="sm" ml={2}>
                ({count})
              </Text>
            </Heading>
          </Flex>
          {expandedGroups[group] ? (
            <Box pb={4} px={4} width="100%">
              <DataTable
                columns={columns}
                data={assets}
                errorMessage={<ErrorAlert error={error} />}
                initialState={tableURLState}
                isLoading={isLoading}
                modelName="Asset"
                onStateChange={setTableURLState}
                total={assets.length}
              />
            </Box>
          ) : undefined}
        </Box>
      ))}
    </VStack>
  );
};
