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
import { useDisclosure } from "@chakra-ui/react";
import { Button, Flex, Heading, Text } from "@chakra-ui/react";
import type { TaskInstanceCollectionResponse, TaskInstanceResponse } from "openapi-gen/requests/types.gen";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { ActionAccordion } from "src/components/ActionAccordion";
import ActionButton from "src/components/ui/ActionButton";
import { Dialog } from "src/components/ui/Dialog";
import { useBulkDeleteTaskInstances } from "src/queries/useBulkDeleteTaskInstances";

type Props = {
  readonly clearSelections: () => void;
  readonly dagId: string;
  readonly dagRunId: string;
  readonly deleteKeys: Array<TaskInstanceResponse>;
};

const DeleteTaskInstancesButton = ({ clearSelections, dagId, dagRunId, deleteKeys }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { deleteTaskInstances, isPending } = useBulkDeleteTaskInstances({
    dagId,
    dagRunId,
    onSuccessConfirm: () => {
      clearSelections();
      onClose();
    },
  });
  const { t: translate } = useTranslation();

  if (deleteKeys.length === 0) {
    return undefined;
  }

  const type = translate("common:taskInstance_other");
  const title = translate("dags:runAndTaskActions.delete.dialog.title", { type });
  const warningText = translate("dags:runAndTaskActions.delete.dialog.warning", { type });
  const deleteButtonText = translate("dags:runAndTaskActions.delete.button", { type });

  const affectedTasks = {
    task_instances: deleteKeys,
    total_entries: deleteKeys.length,
  } as TaskInstanceCollectionResponse;

  return (
    <>
      <ActionButton
        actionName={deleteButtonText}
        colorPalette="red"
        icon={<FiTrash2 />}
        onClick={onOpen}
        text={deleteButtonText}
        variant="outline"
        withText
      />
      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.CloseTrigger />
          <Flex direction="column" gap={4} p={6}>
            <Heading size="lg">{title}</Heading>
            <Text color="fg.error" fontWeight="bold">
              {warningText}
            </Text>
            <ActionAccordion affectedTasks={affectedTasks} />
            <Flex gap={2} justifyContent="flex-end" mt={4}>
              <Button onClick={onClose} variant="outline">
                {translate("common:modal.cancel")}
              </Button>
              <Button colorPalette="red" loading={isPending} onClick={() => deleteTaskInstances(deleteKeys)}>
                <FiTrash2 style={{ marginRight: 8 }} />
                {deleteButtonText}
              </Button>
            </Flex>
          </Flex>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default DeleteTaskInstancesButton;
