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

import {
  useAssetStateServiceDeleteAssetState,
  useAssetStateServiceListAssetStatesKey,
} from "openapi/queries";
import DeleteDialog from "src/components/DeleteDialog";
import { IconButton, toaster } from "src/components/ui";

type DeleteAssetStateButtonProps = {
  readonly assetId: number;
  readonly stateKey: string;
};

const DeleteAssetStateButton = ({ assetId, stateKey }: DeleteAssetStateButtonProps) => {
  const { t: translate } = useTranslation("browse");
  const { onClose, onOpen, open } = useDisclosure();
  const queryClient = useQueryClient();

  const { isPending, mutate } = useAssetStateServiceDeleteAssetState({
    onError: () => {
      toaster.create({
        description: translate("assetState.delete.error"),
        title: translate("assetState.delete.errorTitle"),
        type: "error",
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: [useAssetStateServiceListAssetStatesKey] });
      onClose();
      toaster.create({
        description: translate("assetState.delete.success"),
        title: translate("assetState.delete.successTitle"),
        type: "success",
      });
    },
  });

  return (
    <>
      <IconButton colorPalette="danger" label={translate("assetState.delete.title")} onClick={onOpen}>
        <FiTrash2 />
      </IconButton>

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ assetId, key: stateKey })}
        open={open}
        resourceName={stateKey}
        title={translate("assetState.delete.title")}
        warningText={translate("assetState.delete.warning")}
      />
    </>
  );
};

export default DeleteAssetStateButton;
