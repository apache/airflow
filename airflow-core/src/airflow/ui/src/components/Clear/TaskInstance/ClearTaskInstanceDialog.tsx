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
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import { useDagServiceGetDagDetails } from "openapi/queries";
import type { ClearTaskInstancesBody, TaskInstanceResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { taskInstanceKey } from "src/components/ActionAccordion/columns";
import { useRerunWithLatestVersion } from "src/components/Clear/useRerunWithLatestVersion";
import Time from "src/components/Time";
import { Checkbox, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";
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
  const closeDialog = props.onClose;
  const openDialog = props.open;
  /* eslint-enable react/destructuring-assignment */
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();

  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["downstream"]);

  const onlyFailed = selectedOptions.includes("onlyFailed");
  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");
  const [preventRunningTask, setPreventRunningTask] = useState(true);

  const [note, setNote] = useState<string | null>(taskInstance?.note ?? null);

  useEffect(() => {
    if (openDialog) {
      setNote(taskInstance?.note ?? null);
    }
  }, [openDialog, taskInstance?.note]);

  const onCloseDialog = () => {
    setNote(taskInstance?.note ?? null);
    closeDialog();
  };

  // Get current DAG's bundle version to compare with task instance's DAG version bundle version
  const { data: dagDetails } = useDagServiceGetDagDetails({
    dagId,
  });

  const { dagVersionsDiffer, shouldShowRunOnLatestOption } = getRunOnLatestVersionState({
    latestBundleVersion: dagDetails?.bundle_version,
    latestDagVersionNumber: dagDetails?.latest_dag_version?.version_number,
    selectedBundleVersion: taskInstance?.dag_version?.bundle_version,
    selectedDagVersionNumber: taskInstance?.dag_version?.version_number,
  });

  // dagVersionsDiffer becomes the fallback so the historical "auto-check when versions
  // differ" heuristic still applies when neither DAG-level nor global config is set.
  const { setValue: setRunOnLatestVersion, value: runOnLatestVersion } = useRerunWithLatestVersion({
    dagLevelConfig: dagDetails?.rerun_with_latest_version,
    fallback: dagVersionsDiffer,
  });

  const { isPending, mutate } = useClearTaskInstances({
    dagId,
    dagRunId,
    onSuccessConfirm: onCloseDialog,
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

  // Tasks the user has unticked in the affected list; excluded from the clear.
  const [excludedKeys, setExcludedKeys] = useState<Set<string>>(new Set());

  const toggleTask = (key: string, included: boolean) =>
    setExcludedKeys((prev) => {
      const next = new Set(prev);

      if (included) {
        next.delete(key);
      } else {
        next.add(key);
      }

      return next;
    });

  // The dry run already resolved the full affected set, so on confirm we send those
  // task instances explicitly (minus the unticked ones) with the graph-expansion flags
  // off, instead of re-deriving them from the selected task + upstream/downstream.
  const keptTaskInstances = useMemo(
    () => affectedTasks.task_instances.filter((ti) => !excludedKeys.has(taskInstanceKey(ti))),
    [affectedTasks.task_instances, excludedKeys],
  );

  const checkedTaskIds = useMemo<ClearTaskInstancesBody["task_ids"]>(
    () => keptTaskInstances.map((ti) => (ti.map_index < 0 ? ti.task_id : [ti.task_id, ti.map_index])),
    [keptTaskInstances],
  );

  const hasExclusions = excludedKeys.size > 0;

  return (
    <>
      <Dialog.Root lazyMount onOpenChange={onCloseDialog} open={openDialog ? !open : false}>
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
            <ActionAccordion
              affectedTasks={affectedTasks}
              note={note}
              selection={{ excludedKeys, onToggle: toggleTask }}
              setNote={setNote}
            />
            <Flex
              {...(shouldShowRunOnLatestOption ? { alignItems: "center" } : {})}
              gap={3}
              justifyContent={shouldShowRunOnLatestOption ? "space-between" : "end"}
              mt={3}
            >
              {shouldShowRunOnLatestOption ? (
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
                disabled={affectedTasks.total_entries === 0 || checkedTaskIds?.length === 0}
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
            mapIndex,
            onlyFailed,
            past,
            taskId,
            ...(hasExclusions ? { taskIds: checkedTaskIds } : {}),
            upstream,
          }}
          onClose={onClose}
          onConfirm={() => {
            const noteChanged = note !== (taskInstance?.note ?? null);

            if (hasExclusions) {
              // The affected set is already resolved across (potentially) multiple runs.
              // The clear endpoint only targets one run per request, so group the kept
              // instances by run and fire one run-scoped clear each, with the graph-expansion
              // flags off. This honors per-run exclusions (e.g. keep task X in run 1 but drop
              // it from run 2) that a single flat request cannot express.
              const idsByRun = new Map<string, NonNullable<ClearTaskInstancesBody["task_ids"]>>();

              for (const ti of keptTaskInstances) {
                const ids = idsByRun.get(ti.dag_run_id) ?? [];

                ids.push(ti.map_index < 0 ? ti.task_id : [ti.task_id, ti.map_index]);
                idsByRun.set(ti.dag_run_id, ids);
              }

              for (const [runId, taskIds] of idsByRun) {
                mutate({
                  dagId,
                  requestBody: {
                    dag_run_id: runId,
                    dry_run: false,
                    include_downstream: false,
                    include_future: false,
                    include_past: false,
                    include_upstream: false,
                    note: noteChanged ? note : undefined,
                    only_failed: onlyFailed,
                    run_on_latest_version: runOnLatestVersion,
                    task_ids: taskIds,
                    ...(preventRunningTask ? { prevent_running_task: true } : {}),
                  },
                });
              }
              onCloseDialog();

              return;
            }

            mutate({
              dagId,
              requestBody: {
                dag_run_id: dagRunId,
                dry_run: false,
                include_downstream: downstream,
                include_future: future,
                include_past: past,
                include_upstream: upstream,
                note: noteChanged ? note : undefined,
                only_failed: onlyFailed,
                run_on_latest_version: runOnLatestVersion,
                task_ids: allMapped ? [taskId] : [[taskId, mapIndex as number]],
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

export default ClearTaskInstanceDialog;
