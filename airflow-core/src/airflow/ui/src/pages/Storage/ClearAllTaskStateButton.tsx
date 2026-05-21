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
import { Button, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import DeleteDialog from "src/components/DeleteDialog";
import { toaster } from "src/components/ui";
import { useClearTaskState } from "src/queries/useTaskState";

type ClearAllTaskStateButtonProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly runId: string;
  readonly taskId: string;
};

const ClearAllTaskStateButton = ({ dagId, mapIndex, runId, taskId }: ClearAllTaskStateButtonProps) => {
  const { t: translate } = useTranslation("browse");
  const { onClose, onOpen, open } = useDisclosure();

  const { isPending, mutate } = useClearTaskState({
    onError: () => {
      toaster.create({
        description: translate("taskState.clearAll.error"),
        title: translate("taskState.clearAll.errorTitle"),
        type: "error",
      });
    },
    onSuccess: () => {
      onClose();
      toaster.create({
        description: translate("taskState.clearAll.success"),
        title: translate("taskState.clearAll.successTitle"),
        type: "success",
      });
    },
  });

  return (
    <>
      <Button colorPalette="danger" onClick={onOpen} variant="outline">
        <FiTrash2 /> {translate("taskState.clearAll.title")}
      </Button>

      <DeleteDialog
        confirmationText={translate("taskState.clearAll.confirmation")}
        deleteButtonText={translate("taskState.clearAll.confirm")}
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ dagId, dagRunId: runId, mapIndex, taskId })}
        open={open}
        title={translate("taskState.clearAll.title")}
        warningText={translate("taskState.clearAll.warning")}
      />
    </>
  );
};

export default ClearAllTaskStateButton;
