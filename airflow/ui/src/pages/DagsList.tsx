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

import { ColumnDef } from "@tanstack/react-table";
import { useSearchParams } from "react-router-dom";
import {
  Badge,
  Button,
  ButtonProps,
  Checkbox,
  Heading,
  HStack,
  Input,
  InputGroup,
  InputGroupProps,
  InputLeftElement,
  InputProps,
  InputRightElement,
  Select,
  Spinner,
  VStack,
} from "@chakra-ui/react";
import { Select as ReactSelect } from "chakra-react-select";
import { FiSearch } from "react-icons/fi";

import { DAG } from "openapi/requests/types.gen";
import { useDagServiceGetDags } from "openapi/queries";
import { DataTable } from "../components/DataTable";
import { pluralize } from "../utils/pluralize";
import { useTableURLState } from "../components/DataTable/useTableUrlState";

const SearchBar = ({
  groupProps,
  inputProps,
  buttonProps,
}: {
  groupProps?: InputGroupProps;
  inputProps?: InputProps;
  buttonProps?: ButtonProps;
}) => (
  <InputGroup {...groupProps}>
    <InputLeftElement pointerEvents="none">
      <FiSearch />
    </InputLeftElement>
    <Input placeholder="Search DAGs" pr={150} {...inputProps} />
    <InputRightElement width={150}>
      <Button
        variant="ghost"
        colorScheme="blue"
        width={140}
        height="1.75rem"
        fontWeight="normal"
        {...buttonProps}
      >
        Advanced Search
      </Button>
    </InputRightElement>
  </InputGroup>
);

const columns: ColumnDef<DAG>[] = [
  {
    accessorKey: "dag_id",
    header: "DAG",
    cell: ({ row }) => row.original.dag_display_name,
  },
  {
    accessorKey: "is_paused",
    header: () => "Is Paused",
    enableSorting: false,
  },
  {
    accessorKey: "timetable_description",
    header: () => "Schedule",
    cell: (info) =>
      info.getValue() !== "Never, external triggers only"
        ? info.getValue()
        : undefined,
    enableSorting: false,
  },
  {
    accessorKey: "next_dagrun",
    header: "Next DAG Run",
    enableSorting: false,
  },
  {
    accessorKey: "tags",
    header: () => "Tags",
    cell: ({ row }) => (
      <HStack>
        {row.original.tags?.map((tag) => (
          <Badge key={tag.name}>{tag.name}</Badge>
        ))}
      </HStack>
    ),
    enableSorting: false,
  },
];

const QuickFilterButton = ({ children, ...rest }: ButtonProps) => (
  <Button
    borderRadius={20}
    fontWeight="normal"
    colorScheme="blue"
    variant="outline"
    {...rest}
  >
    {children}
  </Button>
);

const PAUSED_PARAM = "paused";

export const DagsList = () => {
  const cardView = false;
  const [searchParams, setSearchParams] = useSearchParams();

  const showPaused = searchParams.get(PAUSED_PARAM) === "true";

  const { tableURLState, setTableURLState } = useTableURLState();
  const { sorting, pagination } = tableURLState;

  // TODO: update API to accept multiple orderBy params
  const sort = sorting[0];
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const { data, isLoading } = useDagServiceGetDags({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    onlyActive: true,
    paused: showPaused,
    orderBy,
  });

  return (
    <>
      {isLoading && <Spinner />}
      {!isLoading && !!data?.dags && (
        <>
          <VStack alignItems="none">
            <SearchBar
              inputProps={{ isDisabled: true }}
              buttonProps={{ isDisabled: true }}
            />
            <HStack justifyContent="space-between">
              <HStack>
                <HStack>
                  <QuickFilterButton isActive>All</QuickFilterButton>
                  <QuickFilterButton isDisabled>Failed</QuickFilterButton>
                  <QuickFilterButton isDisabled>Running</QuickFilterButton>
                  <QuickFilterButton isDisabled>Successful</QuickFilterButton>
                </HStack>
                <Checkbox
                  isChecked={showPaused}
                  onChange={() => {
                    if (showPaused) searchParams.delete(PAUSED_PARAM);
                    else searchParams.set(PAUSED_PARAM, "true");
                    setSearchParams(searchParams);
                    setTableURLState({
                      sorting,
                      pagination: { ...pagination, pageIndex: 0 },
                    });
                  }}
                >
                  Show Paused DAGs
                </Checkbox>
              </HStack>
              <ReactSelect placeholder="Filter by tag" isDisabled />
            </HStack>
            <HStack justifyContent="space-between">
              <Heading size="md">
                {pluralize("DAG", data.total_entries)}
              </Heading>
              {cardView && (
                <Select
                  placeholder="Sort by…"
                  width="200px"
                  variant="flushed"
                  value={orderBy}
                  onChange={(e) => {
                    setTableURLState({
                      sorting: e.target.value
                        ? [
                            {
                              id: e.target.value.replace("-", ""),
                              desc: e.target.value.startsWith("-"),
                            },
                          ]
                        : [],
                      pagination,
                    });
                  }}
                >
                  <option value="dag_id">Sort by DAG ID (A-Z)</option>
                  <option value="-dag_id">Sort by DAG ID (Z-A)</option>
                </Select>
              )}
            </HStack>
          </VStack>
          <DataTable
            data={data.dags}
            total={data.total_entries}
            columns={columns}
            initialState={tableURLState}
            onStateChange={setTableURLState}
          />
        </>
      )}
    </>
  );
};
