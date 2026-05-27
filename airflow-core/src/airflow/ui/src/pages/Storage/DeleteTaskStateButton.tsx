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
import { useDisclosure } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { useTaskStateServiceDeleteTaskState, useTaskStateServiceListTaskStatesKey } from "openapi/queries";
import DeleteDialog from "src/components/DeleteDialog";
import { IconButton, toaster } from "src/components/ui";

type DeleteTaskStateButtonProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly runId: string;
  readonly stateKey: string;
  readonly taskId: string;
};

const DeleteTaskStateButton = ({ dagId, mapIndex, runId, stateKey, taskId }: DeleteTaskStateButtonProps) => {
  const { t: translate } = useTranslation("browse");
  const { onClose, onOpen, open } = useDisclosure();
  const queryClient = useQueryClient();

  const { isPending, mutate } = useTaskStateServiceDeleteTaskState({
    onError: () => {
      toaster.create({
        description: translate("taskState.delete.error"),
        title: translate("taskState.delete.errorTitle"),
        type: "error",
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: [useTaskStateServiceListTaskStatesKey] });
      onClose();
      toaster.create({
        description: translate("taskState.delete.success"),
        title: translate("taskState.delete.successTitle"),
        type: "success",
      });
    },
  });

  return (
    <>
      <IconButton colorPalette="danger" label={translate("taskState.delete.title")} onClick={onOpen}>
        <FiTrash2 />
      </IconButton>

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ dagId, dagRunId: runId, key: stateKey, mapIndex, taskId })}
        open={open}
        resourceName={stateKey}
        title={translate("taskState.delete.title")}
        warningText={translate("taskState.delete.warning")}
      />
    </>
  );
};

export default DeleteTaskStateButton;
