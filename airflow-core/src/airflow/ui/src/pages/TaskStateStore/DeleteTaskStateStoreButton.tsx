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
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import {
  useTaskStateStoreServiceDeleteTaskStateStore,
  useTaskStateStoreServiceGetTaskStateStoreKey,
  useTaskStateStoreServiceListTaskStateStoreKey,
} from "openapi/queries";
import DeleteDialog from "src/components/DeleteDialog";
import { IconButton } from "src/components/ui";
import { useStoreMutation } from "src/queries/useStoreMutation";

type Props = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly runId: string;
  readonly storeKey: string;
  readonly taskId: string;
};

export const DeleteTaskStateStoreButton = ({ dagId, mapIndex, runId, storeKey, taskId }: Props) => {
  const { t: translate } = useTranslation("dag");
  const { onClose, onOpen, open } = useDisclosure();

  const { isPending, mutate } = useTaskStateStoreServiceDeleteTaskStateStore(
    useStoreMutation({
      invalidationKeys: [
        useTaskStateStoreServiceListTaskStateStoreKey,
        useTaskStateStoreServiceGetTaskStateStoreKey,
      ],
      onSuccessConfirm: onClose,
      operation: "delete",
      resourceName: translate("taskStateStore.title"),
    }),
  );

  return (
    <>
      <IconButton colorPalette="danger" label={translate("taskStateStore.delete")} onClick={onOpen}>
        <FiTrash2 />
      </IconButton>

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ dagId, dagRunId: runId, key: storeKey, mapIndex, taskId })}
        open={open}
        resourceName={storeKey}
        title={translate("taskStateStore.delete")}
        warningText={translate("taskStateStore.deleteWarning")}
      />
    </>
  );
};
