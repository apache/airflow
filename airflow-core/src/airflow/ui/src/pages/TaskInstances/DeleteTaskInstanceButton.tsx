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
import { IconButton, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import DeleteDialog from "src/components/DeleteDialog";
import { Tooltip } from "src/components/ui";
import { useDeleteTaskInstance } from "src/queries/useDeleteTaskInstance";

type DeleteTaskInstanceButtonProps = {
  readonly taskInstance: TaskInstanceResponse;
};

const DeleteTaskInstanceButton = ({ taskInstance }: DeleteTaskInstanceButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();

  const { isPending, mutate: deleteTaskInstance } = useDeleteTaskInstance({
    dagId: taskInstance.dag_id,
    dagRunId: taskInstance.dag_run_id,
    mapIndex: taskInstance.map_index,
    onSuccessConfirm: () => {
      onClose();
    },
    taskId: taskInstance.task_id,
  });

  return (
    <>
      <Tooltip
        content={translate("dags:runAndTaskActions.delete.button", { type: translate("taskInstance_one") })}
      >
        <IconButton
          aria-label={translate("dags:runAndTaskActions.delete.button", {
            type: translate("taskInstance_one"),
          })}
          colorPalette="danger"
          onClick={onOpen}
          size="md"
          variant="ghost"
        >
          <FiTrash2 />
        </IconButton>
      </Tooltip>

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => {
          deleteTaskInstance({
            dagId: taskInstance.dag_id,
            dagRunId: taskInstance.dag_run_id,
            mapIndex: taskInstance.map_index,
            taskId: taskInstance.task_id,
          });
        }}
        open={open}
        resourceName={translate("dags:runAndTaskActions.delete.dialog.resourceName", {
          id: taskInstance.task_id,
          type: translate("taskInstance_one"),
        })}
        title={translate("dags:runAndTaskActions.delete.dialog.title", {
          type: translate("taskInstance_one"),
        })}
        warningText={translate("dags:runAndTaskActions.delete.dialog.warning", {
          type: translate("taskInstance_one"),
        })}
      />
    </>
  );
};

export default DeleteTaskInstanceButton;
