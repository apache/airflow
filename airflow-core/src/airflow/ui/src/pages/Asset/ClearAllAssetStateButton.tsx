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
import { useClearAssetState } from "src/queries/useAssetState";

type ClearAllAssetStateButtonProps = {
  readonly assetId: number;
};

const ClearAllAssetStateButton = ({ assetId }: ClearAllAssetStateButtonProps) => {
  const { t: translate } = useTranslation("browse");
  const { onClose, onOpen, open } = useDisclosure();

  const { isPending, mutate } = useClearAssetState({
    onError: () => {
      toaster.create({
        description: translate("assetState.clearAll.error"),
        title: translate("assetState.clearAll.errorTitle"),
        type: "error",
      });
    },
    onSuccess: () => {
      onClose();
      toaster.create({
        description: translate("assetState.clearAll.success"),
        title: translate("assetState.clearAll.successTitle"),
        type: "success",
      });
    },
  });

  return (
    <>
      <Button colorPalette="danger" onClick={onOpen} variant="outline">
        <FiTrash2 /> {translate("assetState.clearAll.title")}
      </Button>

      <DeleteDialog
        confirmationText={translate("assetState.clearAll.confirmation")}
        deleteButtonText={translate("assetState.clearAll.confirm")}
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ assetId })}
        open={open}
        title={translate("assetState.clearAll.title")}
        warningText={translate("assetState.clearAll.warning")}
      />
    </>
  );
};

export default ClearAllAssetStateButton;
