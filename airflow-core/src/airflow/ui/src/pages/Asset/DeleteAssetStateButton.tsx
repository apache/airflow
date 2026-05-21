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

import DeleteDialog from "src/components/DeleteDialog";
import { IconButton, toaster } from "src/components/ui";
import { useDeleteAssetState } from "src/queries/useAssetState";

type DeleteAssetStateButtonProps = {
  readonly assetId: number;
  readonly stateKey: string;
};

const DeleteAssetStateButton = ({ assetId, stateKey }: DeleteAssetStateButtonProps) => {
  const { t: translate } = useTranslation("browse");
  const { onClose, onOpen, open } = useDisclosure();

  const { isPending, mutate } = useDeleteAssetState({
    onError: () => {
      toaster.create({
        description: translate("assetState.delete.error"),
        title: translate("assetState.delete.errorTitle"),
        type: "error",
      });
    },
    onSuccess: () => {
      onClose();
      toaster.create({
        description: translate("assetState.delete.success"),
        title: translate("assetState.delete.successTitle"),
        type: "success",
      });
    },
  });

  const handleDelete = () => {
    mutate({ assetId, key: stateKey });
  };

  return (
    <>
      <IconButton colorPalette="danger" label={translate("assetState.delete.title")} onClick={onOpen}>
        <FiTrash2 />
      </IconButton>

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={handleDelete}
        open={open}
        resourceName={stateKey}
        title={translate("assetState.delete.title")}
        warningText={translate("assetState.delete.warning")}
      />
    </>
  );
};

export default DeleteAssetStateButton;
