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

import type { DAGRunResponse } from "openapi/requests/types.gen";
import DeleteDialog from "src/components/DeleteDialog";
import ActionButton from "src/components/ui/ActionButton";
import { useDeleteDagRun } from "src/queries/useDeleteDagRun";

type DeleteRunButtonProps = {
  readonly dagRun: DAGRunResponse;
  readonly withText?: boolean;
};

const DeleteRunButton = ({ dagRun, withText = true }: DeleteRunButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();

  const { isPending, mutate: deleteDagRun } = useDeleteDagRun({
    dagId: dagRun.dag_id,
    dagRunId: dagRun.dag_run_id,
    onSuccessConfirm: () => {
      onClose();
    },
  });

  return (
    <>
      <ActionButton
        actionName="Delete DAG Run"
        colorPalette="red"
        icon={<FiTrash2 />}
        onClick={onOpen}
        text="Delete DAG Run"
        variant="solid"
        withText={withText}
      />

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => {
          deleteDagRun({
            dagId: dagRun.dag_id,
            dagRunId: dagRun.dag_run_id,
          });
        }}
        open={open}
        resourceName={`DAG Run ${dagRun.dag_run_id}`}
        title="Delete DAG Run"
        warningText="This will remove all metadata related to the DAG Run."
      />
    </>
  );
};

export default DeleteRunButton;
