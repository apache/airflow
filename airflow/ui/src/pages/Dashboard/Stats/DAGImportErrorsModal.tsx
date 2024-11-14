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
import { VStack, Heading, Badge, Text } from "@chakra-ui/react";

import { Accordion, Dialog } from "src/components/ui";
import { useImportErrors } from "src/queries/useDagsImportErrors";

type ImportDAGErrorModalProps = {
  onClose: () => void;
  open: boolean;
};

export const DAGImportErrorsModal: React.FC<ImportDAGErrorModalProps> = ({
  onClose,
  open,
}) => {
  const { data, error } = useImportErrors();
  const importErrors = data.errors;
  const importErrorsCount = data.total_entries || 0;

  return (
    <Dialog.Root onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              DAG Import Errors
              <Badge
                background="red"
                borderRadius="full"
                color="white"
                ml={2}
                px={2}
              >
                {importErrorsCount}
              </Badge>
            </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body>
          <Accordion.Root collapsible size="md" variant="enclosed">
            {importErrors.map((importError, index) => (
              <Accordion.Item
                key={importError.import_error_id}
                value={importError.filename}
              >
                <Accordion.ItemTrigger cursor="pointer">
                  {index + 1}
                  {". "}
                  {importError.filename}
                </Accordion.ItemTrigger>
                <Accordion.ItemContent>
                  <Text color="gray.600" fontSize="sm" whiteSpace="pre-wrap">
                    <code>{importError.stack_trace}</code>
                  </Text>
                </Accordion.ItemContent>
              </Accordion.Item>
            ))}
          </Accordion.Root>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
