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
import { Input, VStack, Heading, Box, Text } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useXcomServiceCreateXcomEntry, useXcomServiceGetXcomEntriesKey } from "openapi/queries";
import { JsonEditor } from "src/components/JsonEditor";
import { Button, Dialog, toaster } from "src/components/ui";

type AddXComModalProps = {
  readonly dagId: string;
  readonly isOpen: boolean;
  readonly mapIndex?: number;
  readonly onClose: () => void;
  readonly runId: string;
  readonly taskId: string;
};

const AddXComModal = ({ dagId, isOpen, mapIndex = -1, onClose, runId, taskId }: AddXComModalProps) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const queryClient = useQueryClient();
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");

  const { isPending, mutate } = useXcomServiceCreateXcomEntry({
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
      setKey("");
      setValue("");
      toaster.create({
        description: translate("browse:xcom.add.success"),
        title: translate("browse:xcom.add.successTitle"),
        type: "success",
      });
    },
  });

  const onAdd = () => {
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
        key,
        map_index: mapIndex,
        value: parsedValue,
      },
      taskId,
    });
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} size="lg">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <Heading size="lg">{translate("browse:xcom.add.title")}</Heading>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          <VStack gap={4}>
            <Box width="100%">
              <Text fontWeight="bold" mb={2}>
                {translate("browse:xcom.key")}
              </Text>
              <Input onChange={(event) => setKey(event.target.value)} value={key} />
            </Box>
            <Box width="100%">
              <Text fontWeight="bold" mb={2}>
                {translate("browse:xcom.value")}
              </Text>
              <JsonEditor onChange={(val) => setValue(val)} value={value} />
            </Box>
          </VStack>
        </Dialog.Body>
        <Dialog.Footer>
          <Button onClick={onClose} variant="outline">
            {translate("common:modal.cancel")}
          </Button>
          <Button disabled={!key} loading={isPending} onClick={onAdd}>
            {translate("common:modal.add")}
          </Button>
        </Dialog.Footer>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default AddXComModal;
