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
import { useUiServiceDeleteWorker, useUiServiceRequestWorkerShutdown } from "openapi/queries";
import type { Worker } from "openapi/requests/types.gen";
import { useMemo, useState } from "react";

import { toaster } from "src/components/ui";
import {
  bulkWorkerActionBatchSize,
  bulkWorkerDeleteEligibleStates,
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

  const shutdownMutation = useUiServiceRequestWorkerShutdown();
  const deleteMutation = useUiServiceDeleteWorker();

  const shutdownWorkers = useMemo(
    () => selectedWorkers.filter((worker) => bulkWorkerShutdownEligibleStates.has(worker.state)),
    [selectedWorkers],
  );
  const deleteWorkers = useMemo(
    () => selectedWorkers.filter((worker) => bulkWorkerDeleteEligibleStates.has(worker.state)),
    [selectedWorkers],
  );

  const handleBulkAction = async ({
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
  };

  const handleBulkShutdown = async (): Promise<void> => {
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
  };

  const handleBulkDelete = async (): Promise<void> => {
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
  };

  return {
    deleteWorkers,
    handleBulkDelete,
    handleBulkShutdown,
    isBulkDeletePending,
    isBulkShutdownPending,
    shutdownWorkers,
  };
};
