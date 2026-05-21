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
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { useAssetStoreServiceClearAssetStore, useAssetStoreServiceListAssetStoreKey } from "openapi/queries";
import DeleteDialog from "src/components/DeleteDialog";
import { toaster } from "src/components/ui";
import { createErrorToaster } from "src/utils";

type ClearAllAssetStoreButtonProps = {
  readonly assetId: number;
};

export const ClearAllAssetStoreButton = ({ assetId }: ClearAllAssetStoreButtonProps) => {
  const { t: translate } = useTranslation(["assets", "common"]);
  const { onClose, onOpen, open } = useDisclosure();
  const queryClient = useQueryClient();

  const { isPending, mutate } = useAssetStoreServiceClearAssetStore({
    onError: (error) => {
      createErrorToaster(
        error,
        {
          params: { resourceName: translate("assetStore.clearAll.resource") },
          titleKey: "common:toaster.delete.error",
        },
        translate,
      );
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: [useAssetStoreServiceListAssetStoreKey] });
      onClose();
      toaster.create({
        description: translate("common:toaster.delete.success.description", {
          resourceName: translate("assetStore.clearAll.resource"),
        }),
        title: translate("common:toaster.delete.success.title", {
          resourceName: translate("assetStore.clearAll.resource"),
        }),
        type: "success",
      });
    },
  });

  return (
    <>
      <Button colorPalette="danger" onClick={onOpen} variant="outline">
        <FiTrash2 /> {translate("assetStore.clearAll.title")}
      </Button>

      <DeleteDialog
        deleteButtonText={translate("assetStore.clearAll.title")}
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ assetId })}
        open={open}
        resourceName={translate("assetStore.clearAll.resource")}
        title={translate("assetStore.clearAll.title")}
        warningText={translate("assetStore.clearAll.warning")}
      />
    </>
  );
};
