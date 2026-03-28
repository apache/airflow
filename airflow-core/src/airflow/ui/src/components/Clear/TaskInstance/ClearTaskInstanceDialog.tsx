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

import { useDagServiceGetDagDetails } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import Time from "src/components/Time";
import { Checkbox, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useTaskInstanceServiceGetTaskInstance } from "openapi/queries";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";
import { usePatchTaskInstance } from "src/queries/usePatchTaskInstance";
import { isStatePending, useAutoRefresh } from "src/utils";

import ClearTaskInstanceConfirmationDialog from "./ClearTaskInstanceConfirmationDialog";

type Props = {
  readonly allMapped?: boolean;
  readonly dagId: string;
  readonly dagRunId: string;
  readonly mapIndex?: number;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly taskId: string;
};

const ClearTaskInstanceDialog = ({
  allMapped = false,
  dagId,
  dagRunId,
  mapIndex = -1,
  onClose: onCloseDialog,
  open: openDialog,
  taskId,
}: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();

  const effectiveMapIndex = allMapped ? undefined : mapIndex;

  const { isPending, mutate } = useClearTaskInstances({
    dagId,
    dagRunId,
    onSuccessConfirm: onCloseDialog,
  });

  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["downstream"]);

  const onlyFailed = selectedOptions.includes("onlyFailed");
  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");
  const [runOnLatestVersion, setRunOnLatestVersion] = useState(false);
  const [preventRunningTask, setPreventRunningTask] = useState(true);

  const [note, setNote] = useState<string | null>(null);

  const { isPending: isPendingPatchDagRun, mutate: mutatePatchTaskInstance } =
    effectiveMapIndex !== undefined
      ? usePatchTaskInstance({
          dagId,
          dagRunId,
          mapIndex: effectiveMapIndex,
          taskId,
        })
      : { isPending: false, mutate: () => {} };

  const { data: dagDetails } = useDagServiceGetDagDetails({ dagId });

  const refetchInterval = useAutoRefresh({ dagId });

  const taskIds: Array<string | [string, number]> = allMapped
    ? [taskId]
    : [[taskId, mapIndex]];

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
      run_on_latest_version: runOnLatestVersion,
      task_ids: taskIds,
    },
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  const currentDagBundleVersion = dagDetails?.bundle_version;
  const shouldShowBundleVersionOption =
    currentDagBundleVersion !== null && currentDagBundleVersion !== "";

  const dialogTitle = allMapped
    ? translate("dags:runAndTaskActions.clearAllMapped.title", {
        type: translate("taskInstance_other"),
      })
    : translate("dags:runAndTaskActions.clear.title", {
        type: translate("taskInstance_one"),
      });

  return (
    <>
      <Dialog.Root lazyMount onOpenChange={onCloseDialog} open={openDialog ? !open : false} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                <strong>{dialogTitle}:</strong> {taskId}
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
                    disabled: allMapped,
                    label: translate("dags:runAndTaskActions.options.past"),
                    value: "past",
                  },
                  {
                    disabled: allMapped,
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
            <Flex
              {...(shouldShowBundleVersionOption ? { alignItems: "center" } : {})}
              justifyContent={shouldShowBundleVersionOption ? "space-between" : "end"}
              mt={3}
            >
              {shouldShowBundleVersionOption ? (
                <Checkbox
                  checked={runOnLatestVersion}
                  onCheckedChange={(event) => setRunOnLatestVersion(Boolean(event.checked))}
                >
                  {translate("dags:runAndTaskActions.options.runOnLatestVersion")}
                </Checkbox>
              ) : undefined}
              <Checkbox
                checked={preventRunningTask}
                onCheckedChange={(event) => setPreventRunningTask(Boolean(event.checked))}
                style={{ marginRight: "auto" }}
              >
                {translate("dags:runAndTaskActions.options.preventRunningTasks")}
              </Checkbox>
              <Button
                colorPalette="brand"
                disabled={affectedTasks.total_entries === 0}
                loading={isPending || isPendingPatchDagRun}
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
            mapIndex: effectiveMapIndex,
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
                run_on_latest_version: runOnLatestVersion,
                task_ids: taskIds,
                ...(preventRunningTask ? { prevent_running_task: true } : {}),
              },
            });
            if (note !== null && note.length > 0) {
              if (allMapped) {
                for (const ti of affectedTasks.task_instances) {
                  mutatePatchTaskInstance({
                    dagId,
                    dagRunId,
                    mapIndex: ti.map_index,
                    requestBody: { note },
                    taskId,
                  });
                }
              } else {
                mutatePatchTaskInstance({
                  dagId,
                  dagRunId,
                  mapIndex: effectiveMapIndex,
                  requestBody: { note },
                  taskId,
                });
              }
            }
            onCloseDialog();
          }}
          open={open}
          preventRunningTask={preventRunningTask}
        />
      ) : null}
    </>
  );
};

export default ClearTaskInstanceDialog;
