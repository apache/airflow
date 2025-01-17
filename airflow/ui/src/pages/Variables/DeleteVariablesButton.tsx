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
import { Flex, useDisclosure, Text, VStack, Heading, Code } from "@chakra-ui/react";
import { FiTrash, FiTrash2 } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { Button, Dialog } from "src/components/ui";
import { useBulkDeleteVariables } from "src/queries/useBulkDeleteVariables";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly deleteKeys: Array<string>;
};

const DeleteVariablesButton = ({ clearSelections, deleteKeys: variableKeys }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { error, isPending, mutate } = useBulkDeleteVariables({ clearSelections, onSuccessConfirm: onClose });

  return (
    <>
      <Button
        onClick={() => {
          onOpen();
        }}
        size="sm"
        variant="outline"
      >
        <FiTrash2 />
        Delete
      </Button>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">Delete Variable{variableKeys.length > 1 ? "s" : ""}</Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body width="full">
            <Text color="gray.solid" fontSize="md" fontWeight="semibold" mb={4}>
              You are about to delete{" "}
              <strong>
                {variableKeys.length} variable{variableKeys.length > 1 ? "s" : ""}.
              </strong>
              <br />
              <Code mb={2} mt={2} p={4}>
                {variableKeys.join(", ")}
              </Code>
              <br />
              This action is permanent and cannot be undone.{" "}
              <strong>Are you sure you want to proceed?</strong>
            </Text>
            <ErrorAlert error={error} />
            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="red"
                loading={isPending}
                onClick={() => {
                  mutate({
                    requestBody: {
                      actions: [
                        {
                          action: "delete" as const,
                          action_on_non_existence: "fail",
                          entities: variableKeys,
                        },
                      ],
                    },
                  });
                }}
              >
                <FiTrash /> <Text fontWeight="bold">Yes, Delete</Text>
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default DeleteVariablesButton;
