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
import { IconButton } from "@chakra-ui/react";
import { useUiServiceExitWorkerMaintenance } from "openapi/queries";
import { IoMdExit } from "react-icons/io";

interface MaintenanceExitButtonProps {
  onExitMaintenance: (toast: Record<string, string>) => void;
  workerName: string;
}

export const MaintenanceExitButton = ({ onExitMaintenance, workerName }: MaintenanceExitButtonProps) => {
  const exitMaintenanceMutation = useUiServiceExitWorkerMaintenance({
    onError: (error) => {
      onExitMaintenance({
        description: `Unable to exit ${workerName} from maintenance mode: ${error}`,
        title: "Exit Maintenance Mode failed",
        type: "error",
      });
    },
    onSuccess: () => {
      onExitMaintenance({
        description: `Worker ${workerName} was requested to exit maintenance mode.`,
        title: "Maintenance Mode deactivated",
        type: "success",
      });
    },
  });

  const exitMaintenance = () => {
    exitMaintenanceMutation.mutate({ workerName });
  };

  return (
    <IconButton
      size="sm"
      variant="ghost"
      onClick={() => exitMaintenance()}
      aria-label="Exit Maintenance"
      title="Exit Maintenance"
    >
      <IoMdExit />
    </IconButton>
  );
};
