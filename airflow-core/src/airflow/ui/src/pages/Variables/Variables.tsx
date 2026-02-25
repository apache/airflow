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
import { Box, Flex, HStack, Spacer, useDisclosure, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useVariableServiceGetVariables } from "openapi/queries";
import type { VariableResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useRowSelection, type GetColumnsParams } from "src/components/DataTable/useRowSelection";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ExpandCollapseButtons } from "src/components/ExpandCollapseButtons";
import { SearchBar } from "src/components/SearchBar";
import { Tooltip } from "src/components/ui";
import { ActionBar } from "src/components/ui/ActionBar";
import { Checkbox } from "src/components/ui/Checkbox";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig.tsx";
import { TrimText } from "src/utils/TrimText";

import DeleteVariablesButton from "./DeleteVariablesButton";
import ImportVariablesButton from "./ImportVariablesButton";
import AddVariableButton from "./ManageVariable/AddVariableButton";
import DeleteVariableButton from "./ManageVariable/DeleteVariableButton";
import EditVariableButton from "./ManageVariable/EditVariableButton";

type ColumnProps = {
  readonly open: boolean;
  readonly translate: TFunction;
};

const getColumns = ({
  allRowsSelected,
  multiTeam,
  onRowSelect,
  onSelectAll,
  open,
  selectedRows,
  translate,
}: ColumnProps & GetColumnsParams): Array<ColumnDef<VariableResponse>> => {
  const columns: Array<ColumnDef<VariableResponse>> = [
    {
      accessorKey: "select",
      cell: ({ row }) => (
        <Checkbox
          borderWidth={1}
          checked={selectedRows.get(row.original.key)}
          colorPalette="brand"
          onCheckedChange={(event) => onRowSelect(row.original.key, Boolean(event.checked))}
        />
      ),
      enableHiding: false,
      enableSorting: false,
      header: () => (
        <Checkbox
          borderWidth={1}
          checked={allRowsSelected}
          colorPalette="brand"
          onCheckedChange={(event) => onSelectAll(Boolean(event.checked))}
        />
      ),
      meta: {
        skeletonWidth: 10,
      },
    },
    {
      accessorKey: "key",
      cell: ({ row }) => <TrimText isClickable onClickContent={row.original} text={row.original.key} />,
      header: translate("columns.key"),
    },
    {
      accessorKey: "value",
      cell: ({ row }) => (
        <Box minWidth={0} overflowWrap="anywhere" wordBreak="break-word">
          <TrimText
            charLimit={open ? row.original.value.length : undefined}
            showTooltip
            text={row.original.value}
          />
        </Box>
      ),
      header: translate("columns.value"),
    },
    {
      accessorKey: "description",
      cell: ({ row }) => (
        <Box minWidth={0} overflowWrap="anywhere" wordBreak="break-word">
          <TrimText
            charLimit={open ? row.original.description?.length : undefined}
            showTooltip
            text={row.original.description}
          />
        </Box>
      ),
      header: translate("columns.description"),
    },
    {
      accessorKey: "is_encrypted",
      header: translate("variables.columns.isEncrypted"),
    },
    ...(multiTeam
      ? [
          {
            accessorKey: "team_name",
            header: translate("columns.team"),
          },
        ]
      : []),
    {
      accessorKey: "actions",
      cell: ({ row: { original } }) => (
        <Flex justifyContent="end">
          <EditVariableButton disabled={selectedRows.size > 0} variable={original} />
          <DeleteVariableButton deleteKey={original.key} disabled={selectedRows.size > 0} />
        </Flex>
      ),
      enableSorting: false,
      header: "",
      meta: {
        skeletonWidth: 10,
      },
    },
  ];

  return columns;
};

export const Variables = () => {
  const { t: translate } = useTranslation("admin");
  const { setTableURLState, tableURLState } = useTableURLState({
    pagination: { pageIndex: 0, pageSize: 30 },
    sorting: [{ desc: false, id: "key" }],
  }); // To make multiselection smooth
  const [searchParams, setSearchParams] = useSearchParams();
  const { onClose, onOpen, open } = useDisclosure();
  const { NAME_PATTERN, OFFSET }: SearchParamsKeysType = SearchParamsKeys;
  const [variableKeyPattern, setVariableKeyPattern] = useState(searchParams.get(NAME_PATTERN) ?? undefined);
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id === "value" ? "_val" : sort.id}`] : ["-key"];
  const multiTeamEnabled = Boolean(useConfig("multi_team"));

  const { data, error, isFetching, isLoading } = useVariableServiceGetVariables({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    variableKeyPattern: variableKeyPattern ?? undefined,
  });

  const { allRowsSelected, clearSelections, handleRowSelect, handleSelectAll, selectedRows } =
    useRowSelection({
      data: data?.variables,
      getKey: (variable) => variable.key,
    });

  const columns = getColumns({
    allRowsSelected,
    multiTeam: multiTeamEnabled,
    onRowSelect: handleRowSelect,
    onSelectAll: handleSelectAll,
    open,
    selectedRows,
    translate,
  });

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN, value);
    } else {
      searchParams.delete(NAME_PATTERN);
    }
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
    setVariableKeyPattern(value);
  };

  return (
    <>
      <VStack alignItems="none">
        <SearchBar
          defaultValue={variableKeyPattern ?? ""}
          onChange={handleSearchChange}
          placeholder={translate("variables.searchPlaceholder")}
        />
        <HStack gap={4} mt={2}>
          <ImportVariablesButton disabled={selectedRows.size > 0} />
          <Spacer />
          <ExpandCollapseButtons
            collapseLabel={translate("common:expand.collapse")}
            expandLabel={translate("common:expand.expand")}
            onCollapse={onClose}
            onExpand={onOpen}
          />
          <AddVariableButton disabled={selectedRows.size > 0} />
        </HStack>
      </VStack>
      <Box overflow="auto">
        <DataTable
          columns={columns}
          data={data?.variables ?? []}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isFetching={isFetching}
          isLoading={isLoading}
          modelName="admin:variables.variable"
          noRowsMessage={translate("variables.noRowsMessage")}
          onStateChange={setTableURLState}
          total={data?.total_entries ?? 0}
        />
      </Box>
      <ActionBar.Root closeOnInteractOutside={false} open={Boolean(selectedRows.size)}>
        <ActionBar.Content>
          <ActionBar.SelectionTrigger>
            {selectedRows.size} {translate("deleteActions.selected")}
          </ActionBar.SelectionTrigger>
          <ActionBar.Separator />
          <Tooltip content={translate("variables.delete.tooltip")}>
            <DeleteVariablesButton clearSelections={clearSelections} deleteKeys={[...selectedRows.keys()]} />
          </Tooltip>
          <ActionBar.CloseTrigger onClick={clearSelections} />
        </ActionBar.Content>
      </ActionBar.Root>
    </>
  );
};
