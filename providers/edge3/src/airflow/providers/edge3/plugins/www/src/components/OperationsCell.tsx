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
import { Box, Button, HStack, Textarea, VStack } from "@chakra-ui/react";
import type { Worker } from "openapi/requests/types.gen";
import { useState } from "react";

interface MaintenanceFormProps {
  onSubmit: (comment: string) => void;
  onCancel: () => void;
}

const MaintenanceForm = ({ onCancel, onSubmit }: MaintenanceFormProps) => {
  const [comment, setComment] = useState("");

  const handleSubmit = () => {
    if (comment.trim()) {
      onSubmit(comment.trim());
    }
  };

  return (
    <VStack gap={2} align="stretch">
      <Textarea
        placeholder="Enter maintenance comment (required)"
        value={comment}
        onChange={(e) => setComment(e.target.value)}
        required
        maxLength={1024}
        size="sm"
      />
      <HStack gap={2}>
        <Button size="sm" colorScheme="blue" onClick={handleSubmit} disabled={!comment.trim()}>
          Confirm Maintenance
        </Button>
        <Button size="sm" variant="outline" onClick={onCancel}>
          Cancel
        </Button>
      </HStack>
    </VStack>
  );
};

interface OperationsCellProps {
  worker: Worker;
  activeMaintenanceForm: string | null;
  onSetActiveMaintenanceForm: (workerName: string | null) => void;
  onRequestMaintenance: (workerName: string, comment: string) => void;
  onExitMaintenance: (workerName: string) => void;
}

export const OperationsCell = ({
  activeMaintenanceForm,
  onExitMaintenance,
  onRequestMaintenance,
  onSetActiveMaintenanceForm,
  worker,
}: OperationsCellProps) => {
  const workerName = worker.worker_name;
  const state = worker.state;

  if (state === "idle" || state === "running") {
    if (activeMaintenanceForm === workerName) {
      return (
        <MaintenanceForm
          onSubmit={(comment) => onRequestMaintenance(workerName, comment)}
          onCancel={() => onSetActiveMaintenanceForm(null)}
        />
      );
    }
    return (
      <Button size="sm" colorScheme="blue" onClick={() => onSetActiveMaintenanceForm(workerName)}>
        Enter Maintenance
      </Button>
    );
  }

  if (
    state === "maintenance pending" ||
    state === "maintenance mode" ||
    state === "maintenance request" ||
    state === "maintenance exit" ||
    state === "offline maintenance"
  ) {
    return (
      <VStack gap={2} align="stretch">
        <Box fontSize="sm" whiteSpace="pre-wrap">
          {worker.maintenance_comments || "No comment"}
        </Box>
        <Button size="sm" colorScheme="blue" onClick={() => onExitMaintenance(workerName)}>
          Exit Maintenance
        </Button>
      </VStack>
    );
  }

  return null;
};
