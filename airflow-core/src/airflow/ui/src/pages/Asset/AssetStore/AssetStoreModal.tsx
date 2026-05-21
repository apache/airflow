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
import { Box, Button, Heading, Input, Text, VStack } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  useAssetStoreServiceGetAssetStore,
  useAssetStoreServiceListAssetStoreKey,
  useAssetStoreServiceSetAssetStore,
} from "openapi/queries";
import { JsonEditor } from "src/components/JsonEditor";
import { Dialog, ProgressBar, toaster } from "src/components/ui";
import { createErrorToaster } from "src/utils";

type Props = {
  readonly assetId: number;
  readonly isOpen: boolean;
  readonly mode: "add" | "edit";
  readonly onClose: () => void;
  readonly storeKey?: string;
};

const isJsonValid = (val: string) => {
  if (!val.trim()) {
    return false;
  }

  try {
    JSON.parse(val);

    return true;
  } catch {
    return false;
  }
};

export const AssetStoreModal = ({ assetId, isOpen, mode, onClose, storeKey }: Props) => {
  const { t: translate } = useTranslation(["assets", "common"]);
  const queryClient = useQueryClient();
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const isEditMode = mode === "edit";
  const op = isEditMode ? "update" : "create";
  const isValueValid = isJsonValid(value);

  const { data: existingState, isLoading: isFetchingExisting } = useAssetStoreServiceGetAssetStore(
    { assetId, key: storeKey ?? "" },
    undefined,
    { enabled: isEditMode && Boolean(storeKey) },
  );

  useEffect(() => {
    if (isEditMode && existingState !== undefined) {
      setValue(JSON.stringify(existingState.value, null, 2));
    }
  }, [existingState, isEditMode]);

  const { isPending, mutate: setAssetStore } = useAssetStoreServiceSetAssetStore({
    onError: (error) => {
      createErrorToaster(
        error,
        { params: { resourceName: translate("assetStore.title") }, titleKey: `common:toaster.${op}.error` },
        translate,
      );
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: [useAssetStoreServiceListAssetStoreKey] });
      onClose();
      toaster.create({
        description: translate(`common:toaster.${op}.success.description`, {
          resourceName: translate("assetStore.title"),
        }),
        title: translate(`common:toaster.${op}.success.title`, {
          resourceName: translate("assetStore.title"),
        }),
        type: "success",
      });
    },
  });

  const onSave = () => {
    setAssetStore({
      assetId,
      key: isEditMode ? (storeKey ?? "") : key,
      requestBody: { value: JSON.parse(value) },
    });
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} unmountOnExit>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <Heading size="lg">{translate(`assets:assetStore.${mode}`)}</Heading>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          {isFetchingExisting ? (
            <ProgressBar size="xs" />
          ) : (
            <VStack gap={4}>
              <Box width="100%">
                <Text fontWeight="bold" mb={2}>
                  {translate("common:key")}
                </Text>
                {isEditMode ? (
                  <Text>{storeKey}</Text>
                ) : (
                  <Input onChange={(event) => setKey(event.target.value)} value={key} />
                )}
              </Box>

              <Box width="100%">
                <Text fontWeight="bold" mb={2}>
                  {translate("common:value")}
                </Text>
                <JsonEditor onChange={setValue} value={value} />
              </Box>
            </VStack>
          )}
        </Dialog.Body>
        <Dialog.Footer>
          <Button onClick={onClose} variant="outline">
            {translate("common:modal.cancel")}
          </Button>
          <Button
            disabled={isFetchingExisting || !isValueValid || (!isEditMode && key === "")}
            loading={isPending}
            onClick={onSave}
          >
            {translate("common:modal.save")}
          </Button>
        </Dialog.Footer>
      </Dialog.Content>
    </Dialog.Root>
  );
};
