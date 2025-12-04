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
import { Box, Heading, Input, Text, VStack } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  useXcomServiceCreateXcomEntry,
  useXcomServiceGetXcomEntriesKey,
  useXcomServiceGetXcomEntry,
  useXcomServiceGetXcomEntryKey,
  useXcomServiceUpdateXcomEntry,
} from "openapi/queries";
import type { XComResponseNative } from "openapi/requests/types.gen";
import { JsonEditor } from "src/components/JsonEditor";
import { Button, Dialog, ProgressBar, toaster } from "src/components/ui";

type XComModalProps = {
  readonly dagId: string;
  readonly isOpen: boolean;
  readonly mapIndex: number;
  readonly mode: "add" | "edit";
  readonly onClose: () => void;
  readonly runId: string;
  readonly taskId: string;
  readonly xcomKey?: string;
};

const XComModal = ({
  dagId,
  isOpen,
  mapIndex,
  mode,
  onClose,
  runId,
  taskId,
  xcomKey,
}: XComModalProps) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const queryClient = useQueryClient();
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");

  const isEditMode = mode === "edit";

  // Fetch existing XCom data when in edit mode
  const { data, isLoading } = useXcomServiceGetXcomEntry<XComResponseNative>(
    {
      dagId,
      dagRunId: runId,
      deserialize: true,
      mapIndex,
      taskId,
      xcomKey: xcomKey ?? "",
    },
    undefined,
    { enabled: isOpen && isEditMode && Boolean(xcomKey) },
  );

  // Populate form when editing
  useEffect(() => {
    if (isEditMode && data?.value !== undefined) {
      const val = data.value;

      setValue(typeof val === "string" ? val : JSON.stringify(val, undefined, 2));
    }
  }, [data, isEditMode]);

  // Reset form when modal closes
  useEffect(() => {
    if (!isOpen) {
      setKey("");
      setValue("");
    }
  }, [isOpen]);

  // Create mutation
  const { isPending: isCreating, mutate: createXCom } = useXcomServiceCreateXcomEntry({
    onError: () => {
      toaster.create({
        description: translate("browse:xcom.add.error"),
        title: translate("browse:xcom.add.errorTitle"),
        type: "error",
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: [useXcomServiceGetXcomEntriesKey],
      });
      onClose();
      toaster.create({
        description: translate("browse:xcom.add.success"),
        title: translate("browse:xcom.add.successTitle"),
        type: "success",
      });
    },
  });

  // Update mutation
  const { isPending: isUpdating, mutate: updateXCom } = useXcomServiceUpdateXcomEntry({
    onError: () => {
      toaster.create({
        description: translate("browse:xcom.edit.error"),
        title: translate("browse:xcom.edit.errorTitle"),
        type: "error",
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: [useXcomServiceGetXcomEntriesKey],
      });
      await queryClient.invalidateQueries({
        queryKey: [useXcomServiceGetXcomEntryKey],
      });
      onClose();
      toaster.create({
        description: translate("browse:xcom.edit.success"),
        title: translate("browse:xcom.edit.successTitle"),
        type: "success",
      });
    },
  });

  const onSave = () => {
    let parsedValue: unknown = value;

    try {
      parsedValue = JSON.parse(value) as unknown;
    } catch {
      // use string
    }

    if (isEditMode && xcomKey) {
      updateXCom({
        dagId,
        dagRunId: runId,
        requestBody: {
          map_index: mapIndex,
          value: parsedValue,
        },
        taskId,
        xcomKey,
      });
    } else {
      createXCom({
        dagId,
        dagRunId: runId,
        requestBody: {
          key,
          map_index: mapIndex,
          value: parsedValue,
        },
        taskId,
      });
    }
  };

  const isPending = isCreating || isUpdating;
  const title = isEditMode ? translate("browse:xcom.edit.title") : translate("browse:xcom.add.title");

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} size="lg">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <Heading size="lg">{title}</Heading>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          {isLoading ? (
            <ProgressBar size="xs" />
          ) : (
            <VStack gap={4}>
              <Box width="100%">
                <Text fontWeight="bold" mb={2}>
                  {translate("browse:xcom.key")}
                </Text>
                {isEditMode ? (
                  <Text>{xcomKey}</Text>
                ) : (
                  <Input onChange={(event) => setKey(event.target.value)} value={key} />
                )}
              </Box>
              <Box width="100%">
                <Text fontWeight="bold" mb={2}>
                  {translate("browse:xcom.value")}
                </Text>
                <JsonEditor onChange={(val) => setValue(val)} value={value} />
              </Box>
            </VStack>
          )}
        </Dialog.Body>
        <Dialog.Footer>
          <Button onClick={onClose} variant="outline">
            {translate("common:modal.cancel")}
          </Button>
          <Button disabled={isLoading || (!isEditMode && !key)} loading={isPending} onClick={onSave}>
            {translate("common:modal.save")}
          </Button>
        </Dialog.Footer>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default XComModal;
