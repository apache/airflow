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
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { useXcomServiceGetXcomEntriesKey } from "openapi/queries";
import { XcomService } from "openapi/requests/services.gen";
import type { XComResponse } from "openapi/requests/types.gen";
import type { DeleteXcomEntryData } from "openapi/requests/types.gen";
import DeleteDialog from "src/components/DeleteDialog";
import { toaster } from "src/components/ui";

type DeleteXComButtonProps = {
  readonly xcom: XComResponse;
};

const DeleteXComButton = ({ xcom }: DeleteXComButtonProps) => {
  const { t: translate } = useTranslation("browse");
  const { onClose, onOpen, open } = useDisclosure();
  const queryClient = useQueryClient();

  const { isPending, mutate } = useMutation({
    mutationFn: (deleteData: DeleteXcomEntryData) => XcomService.deleteXcomEntry(deleteData),
    onError: () => {
      toaster.create({
        description: translate("xcom.delete.error"),
        title: translate("xcom.delete.errorTitle"),
        type: "error",
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: [useXcomServiceGetXcomEntriesKey],
      });
      onClose();
      toaster.create({
        description: translate("xcom.delete.success"),
        title: translate("xcom.delete.successTitle"),
        type: "success",
      });
    },
  });

  const handleDelete = () => {
    mutate({
      dagId: xcom.dag_id,
      dagRunId: xcom.run_id,
      mapIndex: xcom.map_index,
      taskId: xcom.task_id,
      xcomKey: xcom.key,
    });
  };

  return (
    <>
      <IconButton
        aria-label={translate("xcom.delete.title")}
        colorPalette="danger"
        onClick={onOpen}
        variant="ghost"
      >
        <FiTrash2 />
      </IconButton>

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={handleDelete}
        open={open}
        resourceName={xcom.key}
        title={translate("xcom.delete.title")}
        warningText={translate("xcom.delete.warning")}
      />
    </>
  );
};

export default DeleteXComButton;
