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
import { Button, Flex, Heading, HStack, useDisclosure, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiX } from "react-icons/fi";
import { LuCheck } from "react-icons/lu";

import { ErrorAlert } from "src/components/ErrorAlert";
import { Dialog } from "src/components/ui";
import { useBulkMarkTaskInstances } from "src/queries/useBulkMarkTaskInstances";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedRows: Map<string, boolean>;
};

const BulkMarkTaskInstancesButton = ({ clearSelections, selectedRows }: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const { onClose, onOpen, open } = useDisclosure();
  const { bulkMark, error, isPending } = useBulkMarkTaskInstances({
    clearSelections,
    onSuccessConfirm: onClose,
  });

  return (
    <>
      <Button onClick={onOpen} size="sm" variant="outline">
        {translate("dags:runAndTaskActions.bulkMarkAs.button")}
      </Button>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("dags:runAndTaskActions.bulkMarkAs.dialog.title", {
                  count: selectedRows.size,
                })}
              </Heading>
            </VStack>
          </Dialog.Header>
          <Dialog.CloseTrigger />
          <Dialog.Body width="full">
            <ErrorAlert error={error} />
            <Flex justifyContent="end" mt={3}>
              <HStack>
                <Button
                  colorPalette="success"
                  loading={isPending}
                  onClick={() => bulkMark(selectedRows, "success")}
                  variant="outline"
                >
                  <LuCheck />
                  {translate("common:states.success")}
                </Button>
                <Button
                  colorPalette="danger"
                  loading={isPending}
                  onClick={() => bulkMark(selectedRows, "failed")}
                  variant="outline"
                >
                  <FiX />
                  {translate("common:states.failed")}
                </Button>
              </HStack>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default BulkMarkTaskInstancesButton;
