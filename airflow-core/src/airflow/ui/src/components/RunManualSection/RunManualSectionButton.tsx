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
import type { IconButtonProps } from "@chakra-ui/react";
import { useEffect, useState, type MouseEvent } from "react";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Checkbox, Dialog, IconButton } from "src/components/ui";
import { useRunManualSection } from "src/queries/useRunManualSection";
import { useRunManualSectionDryRun } from "src/queries/useRunManualSectionDryRun";

import { isRunnableManualGate, type ManualSectionTarget } from "./manualSectionTarget";

type Props = {
  readonly taskInstance: TaskInstanceResponse;
};

type ActionProps = {
  readonly buttonProps?: Omit<IconButtonProps, "aria-label" | "children" | "onClick">;
  readonly onButtonClick?: (event: MouseEvent<HTMLButtonElement>) => void;
  readonly target: ManualSectionTarget;
};

export const RunManualSectionAction = ({ buttonProps, onButtonClick, target }: ActionProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();
  const [note, setNote] = useState<string | null>(target.note);
  const [preventRunningTask, setPreventRunningTask] = useState(true);

  useEffect(() => {
    if (open) {
      setNote(target.note);
      setPreventRunningTask(true);
    }
  }, [open, target.note]);

  const { dagId, dagRunId, taskId } = target;
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
      <IconButton
        {...buttonProps}
        label={label}
        onClick={(event) => {
          onButtonClick?.(event);
          onOpen();
        }}
      >
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
                {target.taskDisplayName} <Time datetime={target.startDate} />{" "}
                <StateBadge state={target.state} />
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

const taskInstanceToManualSectionTarget = (taskInstance: TaskInstanceResponse): ManualSectionTarget => ({
  dagId: taskInstance.dag_id,
  dagRunId: taskInstance.dag_run_id,
  mapIndex: taskInstance.map_index,
  note: taskInstance.note,
  operator: taskInstance.operator,
  operatorName: taskInstance.operator_name,
  startDate: taskInstance.start_date,
  state: taskInstance.state,
  taskDisplayName: taskInstance.task_display_name,
  taskId: taskInstance.task_id,
});

const RunManualSectionButton = ({ taskInstance }: Props) => {
  const target = taskInstanceToManualSectionTarget(taskInstance);

  return isRunnableManualGate(target) ? <RunManualSectionAction target={target} /> : null;
};

export default RunManualSectionButton;
