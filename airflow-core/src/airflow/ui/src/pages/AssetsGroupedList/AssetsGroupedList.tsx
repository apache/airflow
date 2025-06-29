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
import { useTranslation } from "react-i18next";
import { FaChevronDown, FaChevronRight, FaChevronUp } from "react-icons/fa";
import { Link as RouterLink } from "react-router-dom";

import { useAssetServiceGetAssetGroups } from "openapi/queries";
import type { AssetResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

import { CreateAssetEvent } from "../Asset/CreateAssetEvent";
import { DependencyPopover } from "../AssetsList/DependencyPopover";

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
  original.consuming_tasks.length ? (
    <DependencyPopover dependencies={original.consuming_tasks} type="Dag" />
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
  const { t: translate } = useTranslation(["assets", "common"]);
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  // Use o novo hook para buscar grupos paginados
  const { data, error, isLoading } = useAssetServiceGetAssetGroups({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  const [search, setSearch] = useState("");
  const [sortAsc, setSortAsc] = useState(true);

  const grouped = useMemo(() => {
    if (!data?.groups) {
      return [];
    }
    const filtered = data.groups.filter(({ group }: { group: string }) =>
      group.toLowerCase().includes(search.toLowerCase()),
    );
    const sortedGroups = filtered.sort((groupA: { group: string }, groupB: { group: string }) =>
      sortAsc ? groupA.group.localeCompare(groupB.group) : groupB.group.localeCompare(groupA.group),
    );

    return sortedGroups;
  }, [data, search, sortAsc]);

  const columns = [
    {
      accessorKey: "name",
      cell: nameCellRenderer,
      header: () => translate("name"),
      size: 300,
    },
    {
      accessorKey: "last_asset_event",
      cell: lastAssetEventCellRenderer,
      enableSorting: false,
      header: () => translate("lastAssetEvent"),
      size: 200,
    },
    {
      accessorKey: "group",
      cell: groupCellRenderer,
      enableSorting: false,
      header: () => translate("group"),
      size: 200,
    },
    {
      accessorKey: "consuming_dags",
      cell: consumingDagsCellRenderer,
      enableSorting: false,
      header: () => translate("consumingDags"),
      size: 200,
    },
    {
      accessorKey: "producing_tasks",
      cell: producingTasksCellRenderer,
      enableSorting: false,
      header: () => translate("producingTasks"),
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
          placeholder={translate("searchGroup", { defaultValue: "Search group" })}
          size="sm"
          value={search}
          width="250px"
        />
        <IconButton
          aria-label={translate("sortGroups", { defaultValue: "Sort groups" })}
          onClick={() => setSortAsc((sortAscValue) => !sortAscValue)}
          size="sm"
          variant="outline"
        >
          {sortAsc ? <FaChevronUp /> : <FaChevronDown />}
        </IconButton>
      </Box>
      {grouped.map(
        ({
          assets,
          count,
          group,
        }: {
          readonly assets: Array<AssetResponse>;
          readonly count: number;
          readonly group: string;
        }) => (
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
                aria-label={
                  expandedGroups[group]
                    ? translate("collapseGroup", { defaultValue: "Collapse group" })
                    : translate("expandGroup", { defaultValue: "Expand group" })
                }
                mr={2}
                onClick={(event) => {
                  event.stopPropagation();
                  handleToggleGroup(group);
                }}
                size="sm"
                variant="ghost"
              >
                {expandedGroups[group] ? <FaChevronDown /> : <FaChevronRight />}
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
                  modelName={translate("common:asset_one")}
                  onStateChange={setTableURLState}
                  total={assets.length}
                />
              </Box>
            ) : undefined}
          </Box>
        ),
      )}
    </VStack>
  );
};
