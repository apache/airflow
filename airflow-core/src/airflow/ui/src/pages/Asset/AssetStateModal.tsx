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
  useAssetStateServiceGetAssetState,
  useAssetStateServiceListAssetStatesKey,
  useAssetStateServiceSetAssetState,
} from "openapi/queries";
import { JsonEditor } from "src/components/JsonEditor";
import { Dialog, ProgressBar, toaster } from "src/components/ui";

type AssetStateModalProps = {
  readonly assetId: number;
  readonly isOpen: boolean;
  readonly mode: "add" | "edit";
  readonly onClose: () => void;
  readonly stateKey?: string;
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

const AssetStateModal = ({ assetId, isOpen, mode, onClose, stateKey }: AssetStateModalProps) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const queryClient = useQueryClient();
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const isEditMode = mode === "edit";
  const isValueValid = isJsonValid(value);

  const { data: existingState, isLoading: isFetchingExisting } = useAssetStateServiceGetAssetState(
    { assetId, key: stateKey ?? "" },
    undefined,
    { enabled: isEditMode && Boolean(stateKey) },
  );

  useEffect(() => {
    if (isEditMode && existingState !== undefined) {
      setValue(existingState.value);
    }
  }, [existingState, isEditMode]);

  const { isPending, mutate: setAssetState } = useAssetStateServiceSetAssetState({
    onError: () => {
      toaster.create({
        description: translate(`browse:assetState.${isEditMode ? "edit" : "add"}.error`),
        title: translate(`browse:assetState.${isEditMode ? "edit" : "add"}.errorTitle`),
        type: "error",
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: [useAssetStateServiceListAssetStatesKey] });
      onClose();
      toaster.create({
        description: translate(`browse:assetState.${isEditMode ? "edit" : "add"}.success`),
        title: translate(`browse:assetState.${isEditMode ? "edit" : "add"}.successTitle`),
        type: "success",
      });
    },
  });

  const onSave = () => {
    setAssetState({
      assetId,
      key: isEditMode ? (stateKey ?? "") : key,
      requestBody: { value },
    });
  };

  const title = isEditMode
    ? translate("browse:assetState.edit.title")
    : translate("browse:assetState.add.title");

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} unmountOnExit>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <Heading size="lg">{title}</Heading>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          {isFetchingExisting ? (
            <ProgressBar size="xs" />
          ) : (
            <VStack gap={4}>
              <Box width="100%">
                <Text fontWeight="bold" mb={2}>
                  {translate("browse:key")}
                </Text>
                {isEditMode ? (
                  <Text>{stateKey}</Text>
                ) : (
                  <Input onChange={(event) => setKey(event.target.value)} value={key} />
                )}
              </Box>

              <Box width="100%">
                <Text fontWeight="bold" mb={2}>
                  {translate("browse:value")}
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

export default AssetStateModal;
