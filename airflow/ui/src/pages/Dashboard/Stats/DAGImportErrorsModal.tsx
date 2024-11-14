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
import { VStack, Heading, Text, Box, Button, HStack } from "@chakra-ui/react";
import { useEffect, useState } from "react";

import type { ImportErrorResponse } from "openapi/requests/types.gen";
import { Accordion, Dialog } from "src/components/ui";
import { useImportErrors } from "src/queries/useDagsImportErrors";

type ImportDAGErrorModalProps = {
  onClose: () => void;
  onErrorCountChange: (count: number) => void;
  open: boolean;
};

const PAGE_LIMIT = 5;

export const DAGImportErrorsModal: React.FC<ImportDAGErrorModalProps> = ({
  onClose,
  onErrorCountChange,
  open,
}) => {
  const [currentPage, setCurrentPage] = useState(0);

  const { data, error } = useImportErrors({
    limit: PAGE_LIMIT,
    offset: currentPage * PAGE_LIMIT,
    orderBy: "import_error_id",
  });

  const importErrors: Array<ImportErrorResponse> = data.import_errors;
  const importErrorsCount: number = data.total_entries || 0;
  const totalPages = Math.ceil(importErrorsCount / PAGE_LIMIT);

  useEffect(() => {
    onErrorCountChange(importErrorsCount);
  }, [importErrorsCount, onErrorCountChange]);

  useEffect(() => {
    if (!open) {
      setCurrentPage(0);
    }
  }, [open]);

  const handleNextPage = () => {
    if (currentPage < totalPages - 1) {
      setCurrentPage((prev) => prev + 1);
    }
  };

  const handlePreviousPage = () => {
    if (currentPage > 0) {
      setCurrentPage((prev) => prev - 1);
    }
  };

  return (
    <Dialog.Root onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4} position="sticky" top={0} zIndex={1}>
            <Heading size="xl">DAG Import Errors</Heading>
            <Text color="gray.500">{`Page ${currentPage + 1} of ${totalPages}`}</Text>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body maxH="500px" overflowY="auto">
          <Accordion.Root collapsible multiple size="md" variant="enclosed">
            {importErrors.map((importError: ImportErrorResponse) => (
              <Accordion.Item
                key={importError.import_error_id}
                value={importError.filename}
              >
                <Accordion.ItemTrigger cursor="pointer">
                  {importError.import_error_id}
                  {". "}
                  {importError.filename}
                </Accordion.ItemTrigger>
                <Accordion.ItemContent>
                  <Text color="red.600" fontSize="sm" whiteSpace="pre-wrap">
                    <code>{importError.stack_trace}</code>
                  </Text>
                </Accordion.ItemContent>
              </Accordion.Item>
            ))}
          </Accordion.Root>
        </Dialog.Body>

        <Box p={6}>
          <HStack justify="space-between">
            <Button
              colorPalette="blue"
              disabled={currentPage === 0}
              onClick={handlePreviousPage}
            >
              Previous
            </Button>
            <Button
              colorPalette="blue"
              disabled={currentPage === totalPages - 1}
              onClick={handleNextPage}
            >
              Next
            </Button>
          </HStack>
        </Box>
      </Dialog.Content>
    </Dialog.Root>
  );
};
