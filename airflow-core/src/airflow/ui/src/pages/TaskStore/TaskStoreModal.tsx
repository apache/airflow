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
import { Box, Button, Heading, Input, RadioCard, Text, VStack } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  useTaskStoreServiceGetTaskStore,
  useTaskStoreServiceGetTaskStoreKey,
  useTaskStoreServiceListTaskStoreKey,
  useTaskStoreServiceSetTaskStore,
} from "openapi/queries";
import { JsonEditor } from "src/components/JsonEditor";
import { Dialog, ProgressBar } from "src/components/ui";
import { useStoreMutation } from "src/queries/useStoreMutation";

type Props = {
  readonly dagId: string;
  readonly isOpen: boolean;
  readonly mapIndex: number;
  readonly mode: "add" | "edit";
  readonly onClose: () => void;
  readonly runId: string;
  readonly storeKey?: string;
  readonly taskId: string;
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

export const TaskStoreModal = ({
  dagId,
  isOpen,
  mapIndex,
  mode,
  onClose,
  runId,
  storeKey,
  taskId,
}: Props) => {
  const { t: translate } = useTranslation(["dag", "common"]);
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [expiresAt, setExpiresAt] = useState<"default" | "never">("default");
  const isEditMode = mode === "edit";
  const isValueValid = isJsonValid(value);

  const { data: existingState, isLoading: isFetchingExisting } = useTaskStoreServiceGetTaskStore(
    { dagId, dagRunId: runId, key: storeKey ?? "", mapIndex, taskId },
    undefined,
    { enabled: isEditMode && Boolean(storeKey) },
  );

  useEffect(() => {
    if (isEditMode && existingState !== undefined) {
      setValue(JSON.stringify(existingState.value, null, 2));
      setExpiresAt(existingState.expires_at === null ? "never" : "default");
    }
  }, [existingState, isEditMode]);

  const { isPending, mutate: setTaskStore } = useTaskStoreServiceSetTaskStore(
    useStoreMutation({
      invalidationKeys: [useTaskStoreServiceListTaskStoreKey, useTaskStoreServiceGetTaskStoreKey],
      onSuccessConfirm: onClose,
      operation: isEditMode ? "update" : "create",
      resourceName: translate("taskStore.title"),
    }),
  );

  const onSave = () => {
    setTaskStore({
      dagId,
      dagRunId: runId,
      key: isEditMode ? (storeKey ?? "") : key,
      mapIndex,
      requestBody: { expires_at: expiresAt === "never" ? null : "default", value: JSON.parse(value) },
      taskId,
    });
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} unmountOnExit>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <Heading size="lg">{translate(`dag:taskStore.${mode}`)}</Heading>
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

              <Box width="100%">
                <Text fontWeight="bold" mb={2}>
                  {translate("dag:taskStore.expiresAt.label")}
                </Text>
                <RadioCard.Root
                  onValueChange={(ev) => setExpiresAt(ev.value as "default" | "never")}
                  value={expiresAt}
                >
                  <RadioCard.Item value="default">
                    <RadioCard.ItemHiddenInput />
                    <RadioCard.ItemControl>
                      <RadioCard.ItemContent>
                        <RadioCard.ItemText>
                          {translate("dag:taskStore.expiresAt.default", { interval: "30 days" })}
                        </RadioCard.ItemText>
                      </RadioCard.ItemContent>
                      <RadioCard.ItemIndicator />
                    </RadioCard.ItemControl>
                  </RadioCard.Item>
                  <RadioCard.Item value="never">
                    <RadioCard.ItemHiddenInput />
                    <RadioCard.ItemControl>
                      <RadioCard.ItemContent>
                        <RadioCard.ItemText>{translate("dag:taskStore.expiresAt.never")}</RadioCard.ItemText>
                      </RadioCard.ItemContent>
                      <RadioCard.ItemIndicator />
                    </RadioCard.ItemControl>
                  </RadioCard.Item>
                  {/* TODO: Add a datetime picker for custom expiry once a picker component is available */}
                  <RadioCard.Item disabled value="custom">
                    <RadioCard.ItemHiddenInput />
                    <RadioCard.ItemControl>
                      <RadioCard.ItemContent>
                        <RadioCard.ItemText>{translate("dag:taskStore.expiresAt.custom")}</RadioCard.ItemText>
                      </RadioCard.ItemContent>
                      <RadioCard.ItemIndicator />
                    </RadioCard.ItemControl>
                  </RadioCard.Item>
                </RadioCard.Root>
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
