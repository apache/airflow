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

import {
  useTaskStoreServiceClearTaskStore,
  useTaskStoreServiceGetTaskStoreKey,
  useTaskStoreServiceListTaskStoreKey,
} from "openapi/queries";
import DeleteDialog from "src/components/DeleteDialog";
import { useStoreMutation } from "src/queries/useStoreMutation";

type Props = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly runId: string;
  readonly taskId: string;
};

export const ClearAllTaskStoreButton = ({ dagId, mapIndex, runId, taskId }: Props) => {
  const { t: translate } = useTranslation("dag");
  const { onClose, onOpen, open } = useDisclosure();

  const { isPending, mutate } = useTaskStoreServiceClearTaskStore(
    useStoreMutation({
      invalidationKeys: [useTaskStoreServiceListTaskStoreKey, useTaskStoreServiceGetTaskStoreKey],
      onSuccessConfirm: onClose,
      operation: "delete",
      resourceName: translate("taskStore.clearAll.resource"),
    }),
  );

  return (
    <>
      <Button colorPalette="danger" onClick={onOpen} variant="outline">
        <FiTrash2 /> {translate("taskStore.clearAll.title")}
      </Button>

      <DeleteDialog
        deleteButtonText={translate("taskStore.clearAll.title")}
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ dagId, dagRunId: runId, mapIndex, taskId })}
        open={open}
        resourceName={translate("taskStore.clearAll.resource")}
        title={translate("taskStore.clearAll.title")}
        warningText={translate("taskStore.clearAll.warning")}
      />
    </>
  );
};
