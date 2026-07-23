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
import { Button, Flex, Heading, VStack, useDisclosure } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Checkbox, Dialog, IconButton } from "src/components/ui";
import { useRunManualSection } from "src/queries/useRunManualSection";
import { useRunManualSectionDryRun } from "src/queries/useRunManualSectionDryRun";

type Props = {
  readonly taskInstance: TaskInstanceResponse;
};

const MANUAL_GATE_OPERATOR_NAME = "ManualGateOperator";

const isRunnableManualGate = (taskInstance: TaskInstanceResponse) =>
  taskInstance.state === "success" &&
  taskInstance.map_index === -1 &&
  (taskInstance.operator_name === MANUAL_GATE_OPERATOR_NAME ||
    taskInstance.operator === MANUAL_GATE_OPERATOR_NAME);

const RunManualSectionAction = ({ taskInstance }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();
  const [note, setNote] = useState<string | null>(taskInstance.note);
  const [preventRunningTask, setPreventRunningTask] = useState(true);

  useEffect(() => {
    if (open) {
      setNote(taskInstance.note);
      setPreventRunningTask(true);
    }
  }, [open, taskInstance.note]);

  const dagId = taskInstance.dag_id;
  const dagRunId = taskInstance.dag_run_id;
  const taskId = taskInstance.task_id;
  const requestBody = {
    note,
    prevent_running_task: preventRunningTask,
  };

  const { data, isPending: isPendingDryRun } = useRunManualSectionDryRun({
    dagId,
    dagRunId,
    options: {
      enabled: open,
      refetchOnMount: "always",
    },
    requestBody,
    taskId,
  });

  const { isPending, mutate } = useRunManualSection({
    dagId,
    dagRunId,
    onSuccess: onClose,
    taskId,
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };
  const label = translate("dags:runAndTaskActions.manualSection.button");

  return (
    <>
      <IconButton label={label} onClick={onOpen}>
        <FiPlay />
      </IconButton>
      <Dialog.Root
        lazyMount
        onOpenChange={(details) => {
          if (!details.open) {
            onClose();
          }
        }}
        open={open}
      >
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                <strong>{translate("dags:runAndTaskActions.manualSection.title")}:</strong>{" "}
                {taskInstance.task_display_name} <Time datetime={taskInstance.start_date} />{" "}
                <StateBadge state={taskInstance.state} />
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body width="full">
            <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
            <Flex alignItems="center" gap={3} justifyContent="space-between" mt={3}>
              <Checkbox
                checked={preventRunningTask}
                onCheckedChange={(event) => setPreventRunningTask(Boolean(event.checked))}
              >
                {translate("dags:runAndTaskActions.options.preventRunningTasks")}
              </Checkbox>
              <Button
                disabled={affectedTasks.total_entries === 0}
                loading={isPending || isPendingDryRun}
                onClick={() => {
                  mutate({
                    dagId,
                    dagRunId,
                    requestBody: {
                      dry_run: false,
                      ...requestBody,
                    },
                    taskId,
                  });
                }}
              >
                <FiPlay /> {translate("modal.confirm")}
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

const RunManualSectionButton = ({ taskInstance }: Props) =>
  isRunnableManualGate(taskInstance) ? <RunManualSectionAction taskInstance={taskInstance} /> : null;

export default RunManualSectionButton;
