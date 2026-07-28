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
import { Box, Button, Flex, Heading, Input, RadioCard, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import tz from "dayjs/plugin/timezone";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  useTaskStateStoreServiceGetTaskStateStore,
  useTaskStateStoreServiceGetTaskStateStoreKey,
  useTaskStateStoreServiceListTaskStateStoreKey,
  useTaskStateStoreServiceSetTaskStateStore,
} from "openapi/queries";
import { DateTimeInput } from "src/components/DateTimeInput";
import { JsonEditor } from "src/components/JsonEditor";
import { Dialog, ProgressBar } from "src/components/ui";
import { useTimezone } from "src/context/timezone";
import { useStoreMutation } from "src/queries/useStoreMutation";

dayjs.extend(tz);

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

export const TaskStateStoreModal = ({
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
  const { selectedTimezone } = useTimezone();
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [expiresAt, setExpiresAt] = useState<"custom" | "default" | "never">("default");
  const [customExpiresAt, setCustomExpiresAt] = useState("");
  const isEditMode = mode === "edit";
  const isValueValid = isJsonValid(value);
  const minDateTime = useMemo(
    () => dayjs().tz(selectedTimezone).format("YYYY-MM-DDTHH:mm"),
    [selectedTimezone],
  );

  const { data: existingState, isLoading: isFetchingExisting } = useTaskStateStoreServiceGetTaskStateStore(
    { dagId, dagRunId: runId, key: storeKey ?? "", mapIndex, taskId },
    undefined,
    { enabled: isEditMode && Boolean(storeKey) },
  );

  useEffect(() => {
    if (isEditMode && existingState !== undefined) {
      setValue(JSON.stringify(existingState.value, null, 2));
      if (existingState.expires_at === null) {
        setExpiresAt("never");
      } else {
        // The API always returns an absolute datetime — there is no way to distinguish
        // the default value from value saved as custom datetime. Show it as
        // Custom pre-filled with the resolved date so the user can adjust it.
        setExpiresAt("custom");
        setCustomExpiresAt(dayjs(existingState.expires_at).tz(selectedTimezone).format("YYYY-MM-DDTHH:mm"));
      }
    }
  }, [existingState, isEditMode, selectedTimezone]);

  const { isPending, mutate: setTaskStore } = useTaskStateStoreServiceSetTaskStateStore(
    useStoreMutation({
      invalidationKeys: [
        useTaskStateStoreServiceListTaskStateStoreKey,
        useTaskStateStoreServiceGetTaskStateStoreKey,
      ],
      onSuccessConfirm: onClose,
      operation: isEditMode ? "update" : "create",
      resourceName: translate("taskStateStore.title"),
    }),
  );

  const getExpiresAt = () => {
    if (expiresAt === "never") {
      return null;
    }
    if (expiresAt === "custom") {
      return dayjs.tz(customExpiresAt, selectedTimezone).toISOString();
    }

    return "default";
  };

  const onSave = () => {
    setTaskStore({
      dagId,
      dagRunId: runId,
      key: isEditMode ? (storeKey ?? "") : key,
      mapIndex,
      requestBody: {
        expires_at: getExpiresAt(),
        value: JSON.parse(value),
      },
      taskId,
    });
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} unmountOnExit>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <Heading size="lg">{translate(`dag:taskStateStore.${mode}`)}</Heading>
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
                  {translate("dag:taskStateStore.expiresAt.label")}
                </Text>
                <RadioCard.Root
                  onValueChange={(ev) => setExpiresAt(ev.value as "custom" | "default" | "never")}
                  value={expiresAt}
                >
                  <Flex gap={2}>
                    <RadioCard.Item flex={1} value="default">
                      <RadioCard.ItemHiddenInput />
                      <RadioCard.ItemControl>
                        <RadioCard.ItemContent>
                          <RadioCard.ItemText>
                            {translate("dag:taskStateStore.expiresAt.default", { interval: "30 days" })}
                          </RadioCard.ItemText>
                        </RadioCard.ItemContent>
                        <RadioCard.ItemIndicator />
                      </RadioCard.ItemControl>
                    </RadioCard.Item>
                    <RadioCard.Item flex={1} value="never">
                      <RadioCard.ItemHiddenInput />
                      <RadioCard.ItemControl>
                        <RadioCard.ItemContent>
                          <RadioCard.ItemText>
                            {translate("dag:taskStateStore.expiresAt.never")}
                          </RadioCard.ItemText>
                        </RadioCard.ItemContent>
                        <RadioCard.ItemIndicator />
                      </RadioCard.ItemControl>
                    </RadioCard.Item>
                    <RadioCard.Item flex={1} value="custom">
                      <RadioCard.ItemHiddenInput />
                      <RadioCard.ItemControl>
                        <RadioCard.ItemContent>
                          <RadioCard.ItemText>
                            {translate("dag:taskStateStore.expiresAt.custom")}
                          </RadioCard.ItemText>
                        </RadioCard.ItemContent>
                        <RadioCard.ItemIndicator />
                      </RadioCard.ItemControl>
                    </RadioCard.Item>
                  </Flex>
                </RadioCard.Root>
                {expiresAt === "custom" && (
                  <DateTimeInput
                    min={minDateTime}
                    mt={2}
                    onChange={(ev) => setCustomExpiresAt(ev.target.value)}
                    value={customExpiresAt}
                  />
                )}
              </Box>
            </VStack>
          )}
        </Dialog.Body>
        <Dialog.Footer>
          <Button onClick={onClose} variant="outline">
            {translate("common:modal.cancel")}
          </Button>
          <Button
            disabled={
              isFetchingExisting ||
              !isValueValid ||
              (!isEditMode && key === "") ||
              (expiresAt === "custom" && !customExpiresAt)
            }
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
