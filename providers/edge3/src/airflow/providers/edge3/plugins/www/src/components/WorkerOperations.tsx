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
import { Box, Flex, VStack } from "@chakra-ui/react";
import type { Worker } from "openapi/requests/types.gen";

import { toaster } from "src/components/ui";

import { AddQueueButton } from "./AddQueueButton";
import { MaintenanceEditCommentButton } from "./MaintenanceEditCommentButton";
import { MaintenanceEnterButton } from "./MaintenanceEnterButton";
import { MaintenanceExitButton } from "./MaintenanceExitButton";
import { RemoveQueueButton } from "./RemoveQueueButton";
import { WorkerDeleteButton } from "./WorkerDeleteButton";
import { WorkerShutdownButton } from "./WorkerShutdownButton";

interface WorkerOperationsProps {
  onOperations: () => void;
  worker: Worker;
}

export const WorkerOperations = ({ onOperations, worker }: WorkerOperationsProps) => {
  const workerName = worker.worker_name;
  const state = worker.state;

  const onWorkerChange = (toast: Record<string, string>) => {
    toaster.create(toast);
    onOperations();
  };

  if (state === "idle" || state === "running") {
    return (
      <Flex justifyContent="end" gap={2}>
        <AddQueueButton onQueueUpdate={onWorkerChange} workerName={workerName} />
        <RemoveQueueButton onQueueUpdate={onWorkerChange} worker={worker} />
        <MaintenanceEnterButton onEnterMaintenance={onWorkerChange} workerName={workerName} />
        <WorkerShutdownButton onShutdown={onWorkerChange} workerName={workerName} />
      </Flex>
    );
  } else if (
    state === "maintenance pending" ||
    state === "maintenance mode" ||
    state === "maintenance request" ||
    state === "offline maintenance"
  ) {
    return (
      <VStack gap={2} align="stretch">
        <Box fontSize="sm" whiteSpace="pre-wrap">
          {worker.maintenance_comments || "No comment"}
        </Box>
        <Flex justifyContent="end" gap={2}>
          <MaintenanceEditCommentButton onEditComment={onWorkerChange} workerName={workerName} />
          <MaintenanceExitButton onExitMaintenance={onWorkerChange} workerName={workerName} />
          {state === "offline maintenance" ? (
            <WorkerDeleteButton onDelete={onWorkerChange} workerName={workerName} />
          ) : (
            <WorkerShutdownButton onShutdown={onWorkerChange} workerName={workerName} />
          )}
        </Flex>
      </VStack>
    );
  } else if (state === "offline" || state === "unknown") {
    return (
      <Flex justifyContent="end">
        <WorkerDeleteButton onDelete={onWorkerChange} workerName={workerName} />
      </Flex>
    );
  }
  return null;
};
