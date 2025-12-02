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

/* eslint-disable max-lines */
import { Box, Flex, Heading, IconButton, Link, useDisclosure } from "@chakra-ui/react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiEdit2, FiPlus, FiTrash2 } from "react-icons/fi";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useXcomServiceGetXcomEntries, useXcomServiceGetXcomEntriesKey } from "openapi/queries";
import { XcomService } from "openapi/requests/services.gen";
import type { DeleteXcomEntryData, XComResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import DeleteDialog from "src/components/DeleteDialog";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ExpandCollapseButtons } from "src/components/ExpandCollapseButtons";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { Button, toaster } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { getTaskInstanceLink } from "src/utils/links";

import AddXComModal from "./AddXComModal";
import EditXComModal from "./EditXComModal";
import { XComEntry } from "./XComEntry";
import { XComFilters } from "./XComFilters";

const {
  DAG_DISPLAY_NAME_PATTERN: DAG_DISPLAY_NAME_PATTERN_PARAM,
  KEY_PATTERN: KEY_PATTERN_PARAM,
  MAP_INDEX: MAP_INDEX_PARAM,
  RUN_ID_PATTERN: RUN_ID_PATTERN_PARAM,
  TASK_ID_PATTERN: TASK_ID_PATTERN_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

type ColumnsProps = {
  readonly onDelete: (xcom: XComResponse) => void;
  readonly onEdit: (xcom: XComResponse) => void;
  readonly open: boolean;
  readonly translate: (key: string) => string;
};

const columns = ({ onDelete, onEdit, open, translate }: ColumnsProps): Array<ColumnDef<XComResponse>> => [
  {
    accessorKey: "key",
    enableSorting: false,
    header: translate("xcom.columns.key"),
  },
  {
    accessorKey: "dag_id",
    cell: ({ row: { original } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}`}>{original.dag_display_name}</RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("xcom.columns.dag"),
  },
  {
    accessorKey: "run_id",
    cell: ({ row: { original } }: { row: { original: XComResponse } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}/runs/${original.run_id}`}>
          <TruncatedText text={original.run_id} />
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("common:runId"),
  },
  {
    accessorKey: "task_display_name",
    cell: ({ row: { original } }: { row: { original: XComResponse } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink
          to={getTaskInstanceLink({
            dagId: original.dag_id,
            dagRunId: original.run_id,
            mapIndex: original.map_index,
            taskId: original.task_id,
          })}
        >
          <TruncatedText text={original.task_display_name} />
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("common:task_one"),
  },
  {
    accessorKey: "map_index",
    enableSorting: false,
    header: translate("common:mapIndex"),
  },
  {
    accessorKey: "timestamp",
    cell: ({ row: { original } }) => <Time datetime={original.timestamp} />,
    enableSorting: false,
    header: translate("dashboard:timestamp"),
  },
  {
    cell: ({ row: { original } }) => (
      <XComEntry
        dagId={original.dag_id}
        mapIndex={original.map_index}
        open={open}
        runId={original.run_id}
        taskId={original.task_id}
        xcomKey={original.key}
      />
    ),
    enableSorting: false,
    header: translate("xcom.columns.value"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <IconButton aria-label={translate("common:edit")} onClick={() => onEdit(original)} variant="ghost">
          <FiEdit2 />
        </IconButton>
        <IconButton
          aria-label={translate("common:delete")}
          colorPalette="danger"
          onClick={() => onDelete(original)}
          variant="ghost"
        >
          <FiTrash2 />
        </IconButton>
      </Flex>
    ),
    enableSorting: false,
    header: "",
  },
];

export const XCom = () => {
  const { dagId = "~", mapIndex = "-1", runId = "~", taskId = "~" } = useParams();
  const { t: translate } = useTranslation(["browse", "common"]);
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;
  const [searchParams] = useSearchParams();
  const { onClose, onOpen, open } = useDisclosure();
  const queryClient = useQueryClient();

  const { onClose: onCloseAdd, onOpen: onOpenAdd, open: openAdd } = useDisclosure();
  const { onClose: onCloseEdit, onOpen: onOpenEdit, open: openEdit } = useDisclosure();
  const { onClose: onCloseDelete, onOpen: onOpenDelete, open: openDelete } = useDisclosure();

  const [selectedXCom, setSelectedXCom] = useState<XComResponse | undefined>(undefined);

  const filteredKey = searchParams.get(KEY_PATTERN_PARAM);
  const filteredDagDisplayName = searchParams.get(DAG_DISPLAY_NAME_PATTERN_PARAM);
  const filteredMapIndex = searchParams.get(MAP_INDEX_PARAM);
  const filteredRunId = searchParams.get(RUN_ID_PATTERN_PARAM);
  const filteredTaskId = searchParams.get(TASK_ID_PATTERN_PARAM);

  const { LOGICAL_DATE_GTE, LOGICAL_DATE_LTE, RUN_AFTER_GTE, RUN_AFTER_LTE } = SearchParamsKeys;
  const logicalDateGte = searchParams.get(LOGICAL_DATE_GTE);
  const logicalDateLte = searchParams.get(LOGICAL_DATE_LTE);
  const runAfterGte = searchParams.get(RUN_AFTER_GTE);
  const runAfterLte = searchParams.get(RUN_AFTER_LTE);

  const apiParams = {
    dagDisplayNamePattern: filteredDagDisplayName ?? undefined,
    dagId,
    dagRunId: runId,
    limit: pagination.pageSize,
    logicalDateGte: logicalDateGte ?? undefined,
    logicalDateLte: logicalDateLte ?? undefined,
    mapIndex:
      filteredMapIndex !== null && filteredMapIndex !== ""
        ? parseInt(filteredMapIndex, 10)
        : mapIndex === "-1"
          ? undefined
          : parseInt(mapIndex, 10),
    offset: pagination.pageIndex * pagination.pageSize,
    runAfterGte: runAfterGte ?? undefined,
    runAfterLte: runAfterLte ?? undefined,
    runIdPattern: filteredRunId ?? undefined,
    taskId,
    taskIdPattern: filteredTaskId ?? undefined,
    xcomKeyPattern: filteredKey ?? undefined,
  };

  const { data, error, isFetching, isLoading } = useXcomServiceGetXcomEntries(apiParams, undefined);

  const { isPending: isDeleting, mutate: deleteXCom } = useMutation({
    mutationFn: (deleteData: DeleteXcomEntryData) => XcomService.deleteXcomEntry(deleteData),
    onError: () => {
      toaster.create({
        description: translate("xcom.delete.error"),
        title: translate("xcom.delete.errorTitle"),
        type: "error",
      });
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({
        queryKey: [useXcomServiceGetXcomEntriesKey],
      });
      onCloseDelete();
      toaster.create({
        description: translate("xcom.delete.success"),
        title: translate("xcom.delete.successTitle"),
        type: "success",
      });
    },
  });

  const handleDelete = () => {
    if (selectedXCom) {
      deleteXCom({
        dagId: selectedXCom.dag_id,
        dagRunId: selectedXCom.run_id,
        mapIndex: selectedXCom.map_index,
        taskId: selectedXCom.task_id,
        xcomKey: selectedXCom.key,
      });
    }
  };

  const onDelete = useCallback(
    (xcom: XComResponse) => {
      setSelectedXCom(xcom);
      onOpenDelete();
    },
    [onOpenDelete],
  );

  const onEdit = useCallback(
    (xcom: XComResponse) => {
      setSelectedXCom(xcom);
      onOpenEdit();
    },
    [onOpenEdit],
  );

  const memoizedColumns = useMemo(
    () =>
      columns({
        onDelete,
        onEdit,
        open,
        translate,
      }),
    [open, translate, onDelete, onEdit],
  );

  const isTaskInstancePage = dagId !== "~" && runId !== "~" && taskId !== "~";

  return (
    <Box>
      {dagId === "~" && runId === "~" && taskId === "~" ? (
        <Heading size="md">{translate("xcom.title")}</Heading>
      ) : undefined}

      <Flex alignItems="center" justifyContent="space-between">
        <XComFilters />
        <Flex gap={2}>
          {isTaskInstancePage ? (
            <Button colorPalette="blue" onClick={onOpenAdd}>
              <FiPlus /> {translate("xcom.add.title")}
            </Button>
          ) : undefined}
          <ExpandCollapseButtons
            collapseLabel={translate("collapseAllExtra")}
            expandLabel={translate("expandAllExtra")}
            onCollapse={onClose}
            onExpand={onOpen}
          />
        </Flex>
      </Flex>

      <ErrorAlert error={error} />
      <DataTable
        columns={memoizedColumns}
        data={data ? data.xcom_entries : []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName={translate("xcom.title")}
        onStateChange={setTableURLState}
        skeletonCount={undefined}
        total={data ? data.total_entries : 0}
      />

      <AddXComModal
        dagId={dagId}
        isOpen={openAdd}
        mapIndex={mapIndex === "~" || mapIndex === "-1" ? -1 : parseInt(mapIndex, 10)}
        onClose={onCloseAdd}
        runId={runId}
        taskId={taskId}
      />

      {selectedXCom ? (
        <EditXComModal
          dagId={selectedXCom.dag_id}
          isOpen={openEdit}
          mapIndex={selectedXCom.map_index}
          onClose={onCloseEdit}
          runId={selectedXCom.run_id}
          taskId={selectedXCom.task_id}
          xcomKey={selectedXCom.key}
        />
      ) : undefined}

      <DeleteDialog
        isDeleting={isDeleting}
        onClose={onCloseDelete}
        onDelete={handleDelete}
        open={openDelete}
        resourceName={selectedXCom ? selectedXCom.key : ""}
        title={translate("xcom.delete.title")}
        warningText={translate("xcom.delete.warning")}
      />
    </Box>
  );
};
