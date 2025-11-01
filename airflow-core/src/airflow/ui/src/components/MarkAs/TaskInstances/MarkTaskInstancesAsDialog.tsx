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
import { Button, Flex, Heading, VStack } from "@chakra-ui/react";
import type {
  TaskInstanceCollectionResponse,
  TaskInstanceResponse,
  TaskInstanceState,
} from "openapi-gen/requests/types.gen";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";
import { Dialog } from "src/components/ui/Dialog";
import { useBulkPatchTaskInstances } from "src/queries/useBulkPatchTaskInstances";

type Props = {
  readonly clearSelections: () => void;
  readonly dagId: string;
  readonly dagRunId: string;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly patchKeys: Array<TaskInstanceResponse>;
  readonly selectedState: TaskInstanceState;
};

const MarkTaskInstancesAsDialog = ({
  clearSelections,
  dagId,
  dagRunId,
  onClose,
  open,
  patchKeys,
  selectedState,
}: Props) => {
  const [note, setNote] = useState<string | null>();

  const { isPending, patchTaskInstances } = useBulkPatchTaskInstances({
    dagId,
    dagRunId,
    onSuccessConfirm: () => {
      clearSelections();
      onClose();
    },
  });
  const { t: translate } = useTranslation();

  const affectedTasks = {
    task_instances: patchKeys,
    total_entries: patchKeys.length,
  } as TaskInstanceCollectionResponse;

  const handlePatch = (state: TaskInstanceState) => {
    const actionValue = state === "failed" ? "set_failed" : "set_success";

    patchTaskInstances(patchKeys, actionValue, { note });
    onClose();
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              <strong>
                {translate("dags:runAndTaskActions.markAs.title", {
                  state: selectedState,
                  type: translate("common:taskInstance_other"),
                })}
                :
              </strong>{" "}
              <StateBadge state={selectedState} />
            </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body width="full">
          <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
          <Flex justifyContent="end" mt={3}>
            <Button colorPalette="blue" loading={isPending} onClick={() => handlePatch(selectedState)}>
              {translate("modal.confirm")}
            </Button>
          </Flex>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default MarkTaskInstancesAsDialog;
