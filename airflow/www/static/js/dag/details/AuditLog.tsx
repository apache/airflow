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

/* global moment */

import React, { useMemo, useRef, useState } from "react";
import {
  Box,
  Flex,
  FormControl,
  FormHelperText,
  FormLabel,
  Input,
  SimpleGrid,
  Button,
} from "@chakra-ui/react";
import { createColumnHelper } from "@tanstack/react-table";
import { snakeCase } from "lodash";

import {
  OptionBase,
  useChakraSelectProps,
  CreatableSelect,
  GroupBase,
  ChakraStylesConfig,
} from "chakra-react-select";

import { useEventLogs } from "src/api";
import { getMetaValue, useOffsetTop } from "src/utils";
import type { DagRun } from "src/types";
import LinkButton from "src/components/LinkButton";
import type { EventLog } from "src/types/api-generated";
import { NewTable } from "src/components/NewTable/NewTable";
import { useTableURLState } from "src/components/NewTable/useTableUrlState";
import { CodeCell, TimeCell } from "src/components/NewTable/NewCells";
import { MdRefresh } from "react-icons/md";

const configExcludedEvents = getMetaValue("excluded_audit_log_events");
const configIncludedEvents = getMetaValue("included_audit_log_events");

interface Props {
  taskId?: string;
  run?: DagRun;
}

interface Option extends OptionBase {
  label: string;
  value: string;
}

const dagId = getMetaValue("dag_id") || undefined;

const columnHelper = createColumnHelper<EventLog>();

const AuditLog = ({ taskId, run }: Props) => {
  const logRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(logRef);
  const { tableURLState, setTableURLState } = useTableURLState({
    sorting: [{ id: "when", desc: true }],
  });
  const [includedEvents, setIncludedEvents] = useState(
    configIncludedEvents.length ? configIncludedEvents.split(",") : []
  );
  const [excludedEvents, setExcludedEvents] = useState(
    configExcludedEvents.length ? configExcludedEvents.split(",") : []
  );

  const sort = tableURLState.sorting[0];
  const orderBy = sort ? `${sort.desc ? "-" : ""}${snakeCase(sort.id)}` : "";

  const { data, isLoading, isFetching, refetch } = useEventLogs({
    dagId,
    taskId,
    runId: run?.runId || undefined,
    before: run?.lastSchedulingDecision || undefined,
    after: run?.queuedAt || undefined,
    orderBy,
    limit: tableURLState.pagination.pageSize,
    offset:
      tableURLState.pagination.pageIndex * tableURLState.pagination.pageSize,
    includedEvents: includedEvents ? includedEvents.join(",") : undefined,
    excludedEvents: excludedEvents ? excludedEvents.join(",") : undefined,
  });

  const columns = useMemo(() => {
    const when = columnHelper.accessor("when", {
      header: "When",
      cell: TimeCell,
    });
    const task = columnHelper.accessor("taskId", {
      header: "Task Id",
      meta: {
        skeletonWidth: 20,
      },
    });
    const runId = columnHelper.accessor("runId", {
      header: "Run Id",
    });
    const rest = [
      columnHelper.accessor("event", {
        header: "Event",
        meta: {
          skeletonWidth: 40,
        },
      }),
      columnHelper.accessor("owner", {
        header: "Owner",
        meta: {
          skeletonWidth: 20,
        },
      }),
      columnHelper.accessor("extra", {
        header: "Extra",
        cell: CodeCell,
      }),
    ];
    return [
      when,
      ...(!run ? [runId] : []),
      ...(!taskId ? [task] : []),
      ...rest,
    ];
  }, [taskId, run]);

  const memoData = useMemo(() => data?.eventLogs, [data?.eventLogs]);

  const chakraStyles: ChakraStylesConfig<Option, true, GroupBase<Option>> = {
    dropdownIndicator: (provided) => ({
      ...provided,
      display: "none",
    }),
    indicatorSeparator: (provided) => ({
      ...provided,
      display: "none",
    }),
    menuList: (provided) => ({
      ...provided,
      py: 0,
    }),
  };

  const excludedEventsSelectProps = useChakraSelectProps<Option, true>({
    isMulti: true,
    tagVariant: "solid",
    value: excludedEvents.map((e) => ({
      label: e,
      value: e,
    })),
    onChange: (options) => {
      setExcludedEvents((options || []).map(({ value }) => value));
    },
    placeholder: "Type to filter an event",
    chakraStyles,
  });

  const includedEventsSelectProps = useChakraSelectProps<Option, true>({
    isMulti: true,
    tagVariant: "solid",
    value: includedEvents.map((e) => ({
      label: e,
      value: e,
    })),
    onChange: (options) => {
      setIncludedEvents((options || []).map(({ value }) => value));
    },
    placeholder: "Type to filter an event",
    chakraStyles,
  });

  return (
    <Box
      height="100%"
      maxHeight={`calc(100% - ${offsetTop}px)`}
      ref={logRef}
      overflowY="auto"
    >
      <Flex justifyContent="right" mb={2}>
        <Button
          leftIcon={<MdRefresh />}
          onClick={() => refetch()}
          variant="outline"
          colorScheme="blue"
          mr={2}
        >
          Refresh
        </Button>
        <LinkButton href={getMetaValue("audit_log_url")}>
          View full cluster Audit Log
        </LinkButton>
      </Flex>
      <SimpleGrid columns={4} columnGap={2}>
        <FormControl>
          <FormLabel>Show Logs After</FormLabel>
          <Input
            type="datetime"
            // @ts-ignore
            placeholder={run?.queuedAt ? moment(run?.queuedAt).format() : ""}
            isDisabled
          />
          {!!run && run?.queuedAt && (
            <FormHelperText>After selected DAG Run Queued At</FormHelperText>
          )}
        </FormControl>
        <FormControl>
          <FormLabel>Show Logs Before</FormLabel>
          <Input
            type="datetime"
            placeholder={
              run?.lastSchedulingDecision
                ? // @ts-ignore
                  moment(run?.lastSchedulingDecision).format()
                : ""
            }
            isDisabled
          />
          {!!run && run?.lastSchedulingDecision && (
            <FormHelperText>
              Before selected DAG Run Last Scheduling Decision
            </FormHelperText>
          )}
        </FormControl>
        <FormControl>
          <FormLabel>Filter by Run ID</FormLabel>
          <Input placeholder={run?.runId} isDisabled />
          <FormHelperText />
        </FormControl>
        <FormControl>
          <FormLabel>Filter by Task ID</FormLabel>
          <Input placeholder={taskId} isDisabled />
          <FormHelperText />
        </FormControl>
        <FormControl>
          <FormLabel>Events to exclude</FormLabel>
          <CreatableSelect {...excludedEventsSelectProps} />
        </FormControl>
        <FormControl>
          <FormLabel>Events to include</FormLabel>
          <CreatableSelect {...includedEventsSelectProps} />
        </FormControl>
      </SimpleGrid>
      <NewTable
        key={`${taskId}-${run?.runId}`}
        data={memoData || []}
        columns={columns}
        isLoading={isLoading}
        isFetching={isFetching}
        initialState={tableURLState}
        onStateChange={setTableURLState}
        resultCount={data?.totalEntries}
      />
    </Box>
  );
};

export default AuditLog;
