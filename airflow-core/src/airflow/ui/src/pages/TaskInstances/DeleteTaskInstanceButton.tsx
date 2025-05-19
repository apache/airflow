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
import { FiTrash2 } from "react-icons/fi";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import DeleteDialog from "src/components/DeleteDialog";
import ActionButton from "src/components/ui/ActionButton";
import { useDeleteTaskInstance } from "src/queries/useDeleteTaskInstance";

type DeleteTaskInstanceButtonProps = {
  readonly taskInstance: TaskInstanceResponse;
  readonly withText?: boolean;
};

const DeleteTaskInstanceButton = ({ taskInstance, withText = true }: DeleteTaskInstanceButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();

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
      <ActionButton
        actionName="Delete Task Instance"
        colorPalette="red"
        icon={<FiTrash2 />}
        onClick={onOpen}
        text="Delete Task Instance"
        variant="solid"
        withText={withText}
      />

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
        resourceName={taskInstance.task_id}
        title="Delete Task Instance"
        warningText="This will remove all metadata related to the Task Instance."
      />
    </>
  );
};

export default DeleteTaskInstanceButton;
