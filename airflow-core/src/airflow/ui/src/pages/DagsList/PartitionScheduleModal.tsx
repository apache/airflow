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
import { Heading, HStack, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { FiDatabase } from "react-icons/fi";

import { usePartitionedDagRunServiceGetPartitionedDagRuns } from "openapi/queries";
import type { PartitionedDagRunResponse } from "openapi/requests/types.gen";
import { AssetProgressCell } from "src/components/AssetProgressCell";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { Dialog } from "src/components/ui";

type PartitionScheduleModalProps = {
  readonly dagId: string;
  readonly onClose: () => void;
  readonly open: boolean;
};

const getColumns = (
  translate: (key: string) => string,
  dagId: string,
): Array<ColumnDef<PartitionedDagRunResponse>> => [
  {
    accessorKey: "partition_key",
    enableSorting: false,
    header: translate("dagRun.mappedPartitionKey"),
  },
  {
    accessorKey: "created_at",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.created_at} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.createdAt"),
  },
  {
    accessorKey: "total_received",
    cell: ({ row }) => (
      <AssetProgressCell
        dagId={dagId}
        partitionKey={row.original.partition_key}
        totalReceived={row.original.total_received}
        totalRequired={row.original.total_required}
      />
    ),
    enableSorting: false,
    header: translate("partitionedDagRunDetail.receivedAssetEvents"),
  },
];

export const PartitionScheduleModal = ({ dagId, onClose, open }: PartitionScheduleModalProps) => {
  const { t: translate } = useTranslation("common");

  const { data, error, isFetching, isLoading } = usePartitionedDagRunServiceGetPartitionedDagRuns(
    { dagId, hasCreatedDagRunId: false },
    undefined,
    { enabled: open },
  );

  const partitionedDagRuns = data?.partitioned_dag_runs ?? [];
  const total = data?.total ?? 0;
  const columns = getColumns(translate, dagId);

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} scrollBehavior="inside" size="xl" unmountOnExit>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <HStack>
            <FiDatabase />
            <Heading size="md">{translate("pendingDagRun", { count: total })}</Heading>
          </HStack>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          <ErrorAlert error={error} />
          <DataTable
            columns={columns}
            data={partitionedDagRuns}
            isFetching={isFetching}
            isLoading={isLoading}
            modelName="partitionedDagRun"
            showRowCountHeading={false}
            total={total}
          />
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
