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
import { Box, Flex, HStack, Spacer, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiShare } from "react-icons/fi";
import { useSearchParams } from "react-router-dom";

import { useVariableServiceGetVariables } from "openapi/queries";
import type { VariableResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useRowSelection, type GetColumnsParams } from "src/components/DataTable/useRowSelection";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { Button, Tooltip } from "src/components/ui";
import { ActionBar } from "src/components/ui/ActionBar";
import { Checkbox } from "src/components/ui/Checkbox";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { TrimText } from "src/utils/TrimText";
import { downloadJson } from "src/utils/downloadJson";

import DeleteVariablesButton from "./DeleteVariablesButton";
import ImportVariablesButton from "./ImportVariablesButton";
import AddVariableButton from "./ManageVariable/AddVariableButton";
import DeleteVariableButton from "./ManageVariable/DeleteVariableButton";
import EditVariableButton from "./ManageVariable/EditVariableButton";

const getColumns = ({
  allRowsSelected,
  onRowSelect,
  onSelectAll,
  selectedRows,
  translate,
}: { translate: TFunction } & GetColumnsParams): Array<ColumnDef<VariableResponse>> => [
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
    cell: ({ row }) => <TrimText showTooltip text={row.original.value} />,
    header: translate("columns.value"),
  },
  {
    accessorKey: "description",
    cell: ({ row }) => <TrimText showTooltip text={row.original.description} />,
    header: translate("columns.description"),
  },
  {
    accessorKey: "is_encrypted",
    header: translate("variables.columns.isEncrypted"),
  },
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

export const Variables = () => {
  const { t: translate } = useTranslation("admin");
  const { setTableURLState, tableURLState } = useTableURLState({
    pagination: { pageIndex: 0, pageSize: 30 },
    sorting: [{ desc: false, id: "key" }],
  }); // To make multiselection smooth
  const [searchParams, setSearchParams] = useSearchParams();
  const { NAME_PATTERN: NAME_PATTERN_PARAM }: SearchParamsKeysType = SearchParamsKeys;
  const [variableKeyPattern, setVariableKeyPattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );
  const [selectedVariables, setSelectedVariables] = useState<Record<string, string | undefined>>({});
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id === "value" ? "_val" : sort.id}`] : ["-key"];

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

  const columns = useMemo(
    () =>
      getColumns({
        allRowsSelected,
        onRowSelect: handleRowSelect,
        onSelectAll: handleSelectAll,
        selectedRows,
        translate,
      }),
    [allRowsSelected, handleRowSelect, handleSelectAll, selectedRows, translate],
  );

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setSearchParams(searchParams);
    setVariableKeyPattern(value);
  };

  useEffect(() => {
    const newSelection: Record<string, string | undefined> = { ...selectedVariables };

    data?.variables.forEach((variable) => {
      if (selectedRows.has(variable.key)) {
        newSelection[variable.key] = variable.value;
      }
    });

    // Filter out keys that are not in selectedRows
    const filteredSelection = Object.keys(newSelection)
      .filter((key) => selectedRows.has(key))
      .reduce<Record<string, string | undefined>>((acc, key) => {
        acc[key] = newSelection[key];

        return acc;
      }, {});

    if (Object.keys(filteredSelection).length !== Object.keys(selectedVariables).length) {
      setSelectedVariables(filteredSelection);
    }
  }, [selectedRows, data, selectedVariables]);

  return (
    <>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={variableKeyPattern ?? ""}
          onChange={handleSearchChange}
          placeHolder={translate("variables.searchPlaceholder")}
        />
        <HStack gap={4} mt={2}>
          <ImportVariablesButton disabled={selectedRows.size > 0} />
          <Spacer />
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
          modelName={translate("common:admin.Variables")}
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
          <Tooltip content={translate("variables.exportTooltip")}>
            <Button
              colorPalette="info"
              onClick={() => downloadJson(selectedVariables, "variables")}
              size="sm"
              variant="outline"
            >
              <FiShare />
              {translate("variables.export")}
            </Button>
          </Tooltip>
          <ActionBar.CloseTrigger onClick={clearSelections} />
        </ActionBar.Content>
      </ActionBar.Root>
    </>
  );
};
