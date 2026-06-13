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
  useAssetStateStoreServiceClearAssetStateStore,
  useAssetStateStoreServiceGetAssetStateStoreKey,
  useAssetStateStoreServiceListAssetStateStoreKey,
} from "openapi/queries";
import DeleteDialog from "src/components/DeleteDialog";
import { useStoreMutation } from "src/queries/useStoreMutation";

type ClearAllAssetStateStoreButtonProps = {
  readonly assetId: number;
};

export const ClearAllAssetStateStoreButton = ({ assetId }: ClearAllAssetStateStoreButtonProps) => {
  const { t: translate } = useTranslation("assets");
  const { onClose, onOpen, open } = useDisclosure();

  const { isPending, mutate } = useAssetStateStoreServiceClearAssetStateStore(
    useStoreMutation({
      invalidationKeys: [
        useAssetStateStoreServiceListAssetStateStoreKey,
        useAssetStateStoreServiceGetAssetStateStoreKey,
      ],
      onSuccessConfirm: onClose,
      operation: "delete",
      resourceName: translate("assetStateStore.clearAll.resource"),
    }),
  );

  return (
    <>
      <Button colorPalette="danger" onClick={onOpen} variant="outline">
        <FiTrash2 /> {translate("assetStateStore.clearAll.title")}
      </Button>

      <DeleteDialog
        deleteButtonText={translate("assetStateStore.clearAll.title")}
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ assetId })}
        open={open}
        resourceName={translate("assetStateStore.clearAll.resource")}
        title={translate("assetStateStore.clearAll.title")}
        warningText={translate("assetStateStore.clearAll.warning")}
      />
    </>
  );
};
