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
import { Button, Flex, Heading, useDisclosure, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { Checkbox, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";
import { isStatePending, useAutoRefresh } from "src/utils";

import ClearTaskInstanceConfirmationDialog from "./ClearTaskInstanceConfirmationDialog";

type Props = {
  readonly dagId: string;
  readonly dagRunId: string;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly taskId: string;
};

const ClearAllMappedTaskInstancesDialog = ({
  dagId,
  dagRunId,
  onClose: onCloseDialog,
  open: openDialog,
  taskId,
}: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();

  const { isPending, mutate } = useClearTaskInstances({
    dagId,
    dagRunId,
    onSuccessConfirm: onCloseDialog,
  });

  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["downstream"]);
  const [note, setNote] = useState<string | null>(null);

  const onlyFailed = selectedOptions.includes("onlyFailed");
  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");
  const [preventRunningTask, setPreventRunningTask] = useState(true);

  const refetchInterval = useAutoRefresh({ dagId });

  // Pass just taskId (without mapIndex) to clear ALL mapped instances
  const { data } = useClearTaskInstancesDryRun({
    dagId,
    options: {
      enabled: openDialog,
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti: TaskInstanceResponse) => isStatePending(ti.state))
          ? refetchInterval
          : false,
      refetchOnMount: "always",
    },
    requestBody: {
      dag_run_id: dagRunId,
      include_downstream: downstream,
      include_future: future,
      include_past: past,
      include_upstream: upstream,
      only_failed: onlyFailed,
      task_ids: [taskId], // Just taskId to clear all map indices
    },
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  return (
    <>
      <Dialog.Root lazyMount onOpenChange={onCloseDialog} open={openDialog ? !open : false} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                <strong>{translate("dags:runAndTaskActions.clearAllMapped.title")}:</strong> {taskId}
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body width="full">
            <Flex justifyContent="center">
              <SegmentedControl
                defaultValues={["downstream"]}
                multiple
                onChange={setSelectedOptions}
                options={[
                  {
                    label: translate("dags:runAndTaskActions.options.past"),
                    value: "past",
                  },
                  {
                    label: translate("dags:runAndTaskActions.options.future"),
                    value: "future",
                  },
                  {
                    label: translate("dags:runAndTaskActions.options.upstream"),
                    value: "upstream",
                  },
                  {
                    label: translate("dags:runAndTaskActions.options.downstream"),
                    value: "downstream",
                  },
                  {
                    label: translate("dags:runAndTaskActions.options.onlyFailed"),
                    value: "onlyFailed",
                  },
                ]}
              />
            </Flex>
            <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
            <Flex justifyContent="space-between" mt={3}>
              <Checkbox
                checked={preventRunningTask}
                onCheckedChange={(event) => setPreventRunningTask(Boolean(event.checked))}
              >
                {translate("dags:runAndTaskActions.options.preventRunningTasks")}
              </Checkbox>
              <Button
                colorPalette="brand"
                disabled={affectedTasks.total_entries === 0}
                loading={isPending}
                onClick={onOpen}
              >
                <CgRedo /> {translate("modal.confirm")}
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
      {open ? (
        <ClearTaskInstanceConfirmationDialog
          dagDetails={{
            dagId,
            dagRunId,
            downstream,
            future,
            mapIndex: -1, // Not applicable for all mapped
            onlyFailed,
            past,
            taskId,
            upstream,
          }}
          onClose={onClose}
          onConfirm={() => {
            mutate({
              dagId,
              requestBody: {
                dag_run_id: dagRunId,
                dry_run: false,
                include_downstream: downstream,
                include_future: future,
                include_past: past,
                include_upstream: upstream,
                only_failed: onlyFailed,
                task_ids: [taskId], // Just taskId to clear all map indices
                ...(preventRunningTask ? { prevent_running_task: true } : {}),
              },
            });
            onCloseDialog();
          }}
          open={open}
          preventRunningTask={preventRunningTask}
        />
      ) : null}
    </>
  );
};

export default ClearAllMappedTaskInstancesDialog;
