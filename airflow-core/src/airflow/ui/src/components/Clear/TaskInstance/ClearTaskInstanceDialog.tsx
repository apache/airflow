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
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import { useDagServiceGetDagDetails } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import Time from "src/components/Time";
import { Checkbox, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";
import { usePatchTaskInstance } from "src/queries/usePatchTaskInstance";
import { isStatePending, useAutoRefresh } from "src/utils";

import ClearTaskInstanceConfirmationDialog from "./ClearTaskInstanceConfirmationDialog";
import { getRunOnLatestVersionState } from "./runOnLatestVersion";

// Discriminated union: callers pass either `allMapped: true` together with
// `dagId`/`dagRunId`/`taskId` (clears every mapped TI of the task), or a full
// `taskInstance` (clears that single TI and reads its display fields). The
// two variants are mutually exclusive at the type level — no defensive
// runtime fallback chains needed in the body.
type Props = (
  | {
      readonly allMapped: true;
      readonly dagId: string;
      readonly dagRunId: string;
      readonly taskId: string;
    }
  | {
      readonly allMapped?: false;
      readonly taskInstance: TaskInstanceResponse;
    }
) & {
  readonly onClose: () => void;
  readonly open: boolean;
};

// react/destructuring-assignment expects every prop access via signature
// destructure, but TypeScript's discriminated-union narrowing needs the union
// kept whole on the parameter — otherwise the `props.allMapped` discriminator
// is severed from `props.dagId` / `props.taskInstance`. Disable the rule for
// the parameter and the prop extraction; everything after uses local
// variables.
/* eslint-disable react/destructuring-assignment */
const ClearTaskInstanceDialog = (props: Props) => {
  const allMapped = props.allMapped === true;
  const dagId = props.allMapped ? props.dagId : props.taskInstance.dag_id;
  const dagRunId = props.allMapped ? props.dagRunId : props.taskInstance.dag_run_id;
  const taskId = props.allMapped ? props.taskId : props.taskInstance.task_id;
  const mapIndex: number | undefined = props.allMapped ? undefined : props.taskInstance.map_index;
  const taskInstance: TaskInstanceResponse | undefined = props.allMapped ? undefined : props.taskInstance;
  const onCloseDialog = props.onClose;
  const openDialog = props.open;
  /* eslint-enable react/destructuring-assignment */
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();

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
  const userToggledRunOnLatestRef = useRef(false);
  const [preventRunningTask, setPreventRunningTask] = useState(true);

  const [note, setNote] = useState<string | null>(taskInstance?.note ?? null);
  const { isPending: isPendingPatchDagRun, mutate: mutatePatchTaskInstance } = usePatchTaskInstance({
    dagId,
    dagRunId,
    taskId,
  });

  // Get current DAG's bundle version to compare with task instance's DAG version bundle version
  const { data: dagDetails } = useDagServiceGetDagDetails({
    dagId,
  });

  const refetchInterval = useAutoRefresh({ dagId });

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
      task_ids: allMapped ? [taskId] : [[taskId, mapIndex as number]],
    },
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  const { dagVersionsDiffer, shouldShowRunOnLatestOption } = getRunOnLatestVersionState({
    latestBundleVersion: dagDetails?.bundle_version,
    latestDagVersionNumber: dagDetails?.latest_dag_version?.version_number,
    selectedBundleVersion: taskInstance?.dag_version?.bundle_version,
    selectedDagVersionNumber: taskInstance?.dag_version?.version_number,
  });

  useEffect(() => {
    if (!openDialog) {
      userToggledRunOnLatestRef.current = false;
    } else if (!userToggledRunOnLatestRef.current) {
      setRunOnLatestVersion(dagVersionsDiffer);
    }
  }, [openDialog, dagVersionsDiffer]);

  return (
    <>
      <Dialog.Root lazyMount onOpenChange={onCloseDialog} open={openDialog ? !open : false} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                <strong>
                  {allMapped
                    ? translate("dags:runAndTaskActions.clearAllMapped.title")
                    : translate("dags:runAndTaskActions.clear.title", {
                        type: translate("taskInstance_one"),
                      })}
                  :
                </strong>{" "}
                {allMapped ? (
                  taskId
                ) : (
                  <>
                    {taskInstance?.task_display_name} <Time datetime={taskInstance?.start_date} />
                  </>
                )}
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
                    disabled: allMapped || taskInstance?.logical_date === null,
                    label: translate("dags:runAndTaskActions.options.past"),
                    value: "past",
                  },
                  {
                    disabled: allMapped || taskInstance?.logical_date === null,
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
              {...(shouldShowRunOnLatestOption ? { alignItems: "center" } : {})}
              gap={3}
              justifyContent={shouldShowRunOnLatestOption ? "space-between" : "end"}
              mt={3}
            >
              {shouldShowRunOnLatestOption ? (
                <Checkbox
                  checked={runOnLatestVersion}
                  onCheckedChange={(event) => {
                    userToggledRunOnLatestRef.current = true;
                    setRunOnLatestVersion(Boolean(event.checked));
                  }}
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
            mapIndex,
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
                task_ids: allMapped ? [taskId] : [[taskId, mapIndex as number]],
                ...(preventRunningTask ? { prevent_running_task: true } : {}),
              },
            });
            if (note !== (taskInstance?.note ?? null)) {
              mutatePatchTaskInstance({
                dagId,
                dagRunId,
                ...(allMapped ? {} : { mapIndex }),
                requestBody: { note },
                taskId,
              });
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
