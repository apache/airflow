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
import {
  useUiServiceDeleteWorker,
  useUiServiceExitWorkerMaintenance,
  useUiServiceRequestWorkerMaintenance,
  useUiServiceRequestWorkerShutdown,
} from "openapi/queries";
import type { Worker } from "openapi/requests/types.gen";
import { useCallback, useMemo, useState } from "react";

import { toaster } from "src/components/ui";
import {
  bulkWorkerActionBatchSize,
  bulkWorkerDeleteEligibleStates,
  bulkWorkerMaintenanceEnterEligibleStates,
  bulkWorkerMaintenanceExitEligibleStates,
  bulkWorkerShutdownEligibleStates,
} from "src/constants";

type UseBulkWorkerActionsProps = {
  readonly onClearSelection: VoidFunction;
  readonly onOperations: () => void;
  readonly selectedWorkers: Array<Worker>;
};

type BulkActionResult = {
  failedWorkers: Array<string>;
  successCount: number;
};

type BulkActionConfig = {
  readonly failureTitle: string;
  readonly setPending: (pending: boolean) => void;
  readonly successToast: (successCount: number) => {
    description: string;
    title: string;
    type: "success";
  };
  readonly workers: Array<Worker>;
  readonly workerMutation: (worker: Worker) => Promise<unknown>;
};

const getFailureDescription = (
  failedWorkers: Array<string>,
): string => {
  const maxFailuresToDisplay = 5;
  const displayedFailedWorkers = failedWorkers.slice(0, maxFailuresToDisplay);
  const additionalFailedWorkersCount = failedWorkers.length - displayedFailedWorkers.length;
  const additionalFailuresMessage =
    additionalFailedWorkersCount > 0 ? ` and ${additionalFailedWorkersCount} more` : "";

  return `Failed for ${failedWorkers.length} worker(s): ${displayedFailedWorkers.join(", ")}${additionalFailuresMessage}.`;
};

const getBulkActionResult = (
  workers: Array<Worker>,
  results: Array<PromiseSettledResult<unknown>>,
): BulkActionResult => {
  const failedWorkers = results.flatMap((result, index) =>
    result.status === "rejected" ? [workers[index]?.worker_name ?? "unknown"] : [],
  );

  return {
    failedWorkers,
    successCount: workers.length - failedWorkers.length,
  };
};

const runMutationsInBatches = async (
  workers: Array<Worker>,
  workerMutation: (worker: Worker) => Promise<unknown>,
): Promise<Array<PromiseSettledResult<unknown>>> => {
  const results: Array<PromiseSettledResult<unknown>> = [];

  for (let index = 0; index < workers.length; index += bulkWorkerActionBatchSize) {
    const batchWorkers = workers.slice(index, index + bulkWorkerActionBatchSize);
    const batchResults = await Promise.allSettled(batchWorkers.map((worker) => workerMutation(worker)));

    results.push(...batchResults);
  }

  return results;
};

export const useBulkWorkerActions = ({
  onClearSelection,
  onOperations,
  selectedWorkers,
}: UseBulkWorkerActionsProps) => {
  const [isBulkDeletePending, setIsBulkDeletePending] = useState(false);
  const [isBulkShutdownPending, setIsBulkShutdownPending] = useState(false);
  const [isBulkMaintenanceEnterPending, setIsBulkMaintenanceEnterPending] = useState(false);
  const [isBulkMaintenanceExitPending, setIsBulkMaintenanceExitPending] = useState(false);

  const shutdownMutation = useUiServiceRequestWorkerShutdown();
  const deleteMutation = useUiServiceDeleteWorker();
  const maintenanceEnterMutation = useUiServiceRequestWorkerMaintenance();
  const maintenanceExitMutation = useUiServiceExitWorkerMaintenance();

  const shutdownWorkers = useMemo(
    () => selectedWorkers.filter((worker) => bulkWorkerShutdownEligibleStates.has(worker.state)),
    [selectedWorkers],
  );
  const deleteWorkers = useMemo(
    () => selectedWorkers.filter((worker) => bulkWorkerDeleteEligibleStates.has(worker.state)),
    [selectedWorkers],
  );
  const maintenanceEnterWorkers = useMemo(
    () => selectedWorkers.filter((worker) => bulkWorkerMaintenanceEnterEligibleStates.has(worker.state)),
    [selectedWorkers],
  );
  const maintenanceExitWorkers = useMemo(
    () => selectedWorkers.filter((worker) => bulkWorkerMaintenanceExitEligibleStates.has(worker.state)),
    [selectedWorkers],
  );

  const handleBulkAction = useCallback(async ({
    failureTitle,
    setPending,
    successToast,
    workers,
    workerMutation,
  }: BulkActionConfig): Promise<void> => {
    setPending(true);

    try {
      const results = await runMutationsInBatches(workers, workerMutation);
      const { failedWorkers, successCount } = getBulkActionResult(workers, results);

      if (successCount > 0) {
        toaster.create(successToast(successCount));
      }

      if (failedWorkers.length > 0) {
        toaster.create({
          description: getFailureDescription(failedWorkers),
          title: failureTitle,
          type: "error",
        });
      }

      if (successCount > 0) {
        onOperations();
        onClearSelection();
      }
    } finally {
      setPending(false);
    }
  }, [onClearSelection, onOperations]);

  const handleBulkShutdown = useCallback(async (): Promise<void> => {
    await handleBulkAction({
      failureTitle: "Bulk Shutdown Partially Failed",
      setPending: setIsBulkShutdownPending,
      successToast: (successCount) => ({
        description: `Shutdown requested for ${successCount} worker(s).`,
        title: "Bulk Shutdown Requested",
        type: "success",
      }),
      workers: shutdownWorkers,
      workerMutation: (worker) => shutdownMutation.mutateAsync({ workerName: worker.worker_name }),
    });
  }, [handleBulkAction, shutdownMutation, shutdownWorkers]);

  const handleBulkDelete = useCallback(async (): Promise<void> => {
    await handleBulkAction({
      failureTitle: "Bulk Delete Partially Failed",
      setPending: setIsBulkDeletePending,
      successToast: (successCount) => ({
        description: `${successCount} worker(s) deleted.`,
        title: "Bulk Delete Completed",
        type: "success",
      }),
      workers: deleteWorkers,
      workerMutation: (worker) => deleteMutation.mutateAsync({ workerName: worker.worker_name }),
    });
  }, [deleteWorkers, deleteMutation, handleBulkAction]);

  const handleBulkMaintenanceEnter = useCallback(async (comment: string): Promise<void> => {
    await handleBulkAction({
      failureTitle: "Bulk Maintenance Enter Partially Failed",
      setPending: setIsBulkMaintenanceEnterPending,
      successToast: (successCount) => ({
        description: `Maintenance mode requested for ${successCount} worker(s).`,
        title: "Bulk Maintenance Enter Requested",
        type: "success",
      }),
      workers: maintenanceEnterWorkers,
      workerMutation: (worker) =>
        maintenanceEnterMutation.mutateAsync({
          requestBody: { maintenance_comment: comment },
          workerName: worker.worker_name,
        }),
    });
  }, [handleBulkAction, maintenanceEnterMutation, maintenanceEnterWorkers]);

  const handleBulkMaintenanceExit = useCallback(async (): Promise<void> => {
    await handleBulkAction({
      failureTitle: "Bulk Maintenance Exit Partially Failed",
      setPending: setIsBulkMaintenanceExitPending,
      successToast: (successCount) => ({
        description: `${successCount} worker(s) requested to exit maintenance mode.`,
        title: "Bulk Maintenance Exit Requested",
        type: "success",
      }),
      workers: maintenanceExitWorkers,
      workerMutation: (worker) => maintenanceExitMutation.mutateAsync({ workerName: worker.worker_name }),
    });
  }, [handleBulkAction, maintenanceExitMutation, maintenanceExitWorkers]);

  return {
    deleteWorkers,
    handleBulkDelete,
    handleBulkMaintenanceEnter,
    handleBulkMaintenanceExit,
    handleBulkShutdown,
    isBulkDeletePending,
    isBulkMaintenanceEnterPending,
    isBulkMaintenanceExitPending,
    isBulkShutdownPending,
    maintenanceEnterWorkers,
    maintenanceExitWorkers,
    shutdownWorkers,
  };
};
