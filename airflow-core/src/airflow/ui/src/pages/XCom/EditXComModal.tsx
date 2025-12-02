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
import { Box, Heading, VStack, Text } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { useXcomServiceGetXcomEntry, useXcomServiceUpdateXcomEntry } from "openapi/queries";
import type { XComResponseNative } from "openapi/requests/types.gen";
import { JsonEditor } from "src/components/JsonEditor";
import { Button, Dialog, toaster, ProgressBar } from "src/components/ui";

type EditXComModalProps = {
  readonly dagId: string;
  readonly isOpen: boolean;
  readonly mapIndex: number;
  readonly onClose: () => void;
  readonly runId: string;
  readonly taskId: string;
  readonly xcomKey: string;
};

const EditXComModal = ({ dagId, isOpen, mapIndex, onClose, runId, taskId, xcomKey }: EditXComModalProps) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const [value, setValue] = useState("");

  const { data, isLoading } = useXcomServiceGetXcomEntry<XComResponseNative>(
    {
      dagId,
      dagRunId: runId,
      deserialize: true,
      mapIndex,
      taskId,
      xcomKey,
    },
    undefined,
    { enabled: isOpen },
  );

  useEffect(() => {
    if (data?.value !== undefined) {
      const val = data.value;

      setValue(typeof val === "string" ? val : JSON.stringify(val, undefined, 2));
    }
  }, [data, isOpen]);

  const { isPending, mutate } = useXcomServiceUpdateXcomEntry({
    onError: () => {
      toaster.create({
        description: translate("browse:xcom.edit.error"),
        title: translate("browse:xcom.edit.errorTitle"),
        type: "error",
      });
    },
    onSuccess: () => {
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

    mutate({
      dagId,
      dagRunId: runId,
      requestBody: {
        map_index: mapIndex,
        value: parsedValue,
      },
      taskId,
      xcomKey,
    });
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} size="lg">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <Heading size="lg">{translate("browse:xcom.edit.title")}</Heading>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          {isLoading ? (
            <ProgressBar size="xs" />
          ) : (
            <VStack gap={4}>
              <Box width="100%">
                <Text fontWeight="bold" mb={2}>
                  {translate("browse:xcom.key")}: {xcomKey}
                </Text>
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
          <Button disabled={isLoading} loading={isPending} onClick={onSave}>
            {translate("common:modal.save")}
          </Button>
        </Dialog.Footer>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default EditXComModal;
