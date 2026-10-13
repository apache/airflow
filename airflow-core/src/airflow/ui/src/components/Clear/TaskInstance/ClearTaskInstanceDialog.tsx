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
import { useEffect, useState } from "react";

import { Button, Flex, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import { useDagRunServiceGetDagRun, useDagServiceGetDagDetails } from "openapi/queries";
import type {
  ClearTaskInstancesBody,
  DagVersionResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import { Checkbox, Modal, SegmentedControl } from "src/system-components";

import { ActionAccordion } from "src/components/ActionAccordion";
import { taskInstanceKey } from "src/components/ActionAccordion/columns";
import { useRerunWithLatestVersion } from "src/components/Clear/useRerunWithLatestVersion";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

import {
  useClearKeepTaskStateDefault,
  useClearPreventRunningTaskDefault,
  useClearTaskInstanceDefaultOptions,
} from "src/hooks/useUserSettings";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRuns } from "src/queries/useClearTaskInstancesDryRun";
import { isStatePending, useAutoRefresh } from "src/utils";

import ClearTaskInstanceConfirmationDialog from "./ClearTaskInstanceConfirmationDialog";
import { getRunOnLatestVersionState } from "./runOnLatestVersion";

/**
 * What the dialog needs to know about a task instance to clear it. A `TaskInstanceResponse`
 * satisfies it, as does an Execution tab row once its loop membership is resolved.
 */
export type ClearTarget = {
  readonly dag_id: string;
  readonly dag_run_id: string;
  readonly dag_version?: Pick<DagVersionResponse, "bundle_version" | "version_number"> | null;
  readonly dag_version_id?: string | null;
  readonly id: string;
  readonly in_loop: boolean;
  readonly logical_date?: string | null;
  readonly map_index: number;
  readonly note?: string | null;
  readonly region_index?: number;
  readonly task_id: string;
};

type RunGroup = {
  readonly dagId: string;
  readonly dagRunId: string;
  readonly targets: Array<ClearTarget>;
};

type ClearRequest = {
  dagId: string;
  requestBody: ClearTaskInstancesBody;
};

// Discriminated union: callers pass either `allMapped: true` together with
// `dagId`/`dagRunId`/`taskId` (clears every mapped TI of the task), a full
// `taskInstance` (clears that single TI and reads its display fields), or
// `taskInstances` (clears a selection, possibly spanning runs and Dags). The
// variants are mutually exclusive at the type level — no defensive
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
  | {
      readonly allMapped?: false;
      readonly taskInstances: Array<ClearTarget>;
    }
) & {
  readonly onCleared?: () => void;
  readonly onClose: () => void;
  readonly open: boolean;
};

const getRunGroups = (targets: Array<ClearTarget>): Array<RunGroup> => {
  const groups = new Map<string, RunGroup>();

  for (const target of targets) {
    const key = JSON.stringify([target.dag_id, target.dag_run_id]);
    const group = groups.get(key) ?? { dagId: target.dag_id, dagRunId: target.dag_run_id, targets: [] };

    group.targets.push(target);
    groups.set(key, group);
  }

  return [...groups.values()];
};

const LATER_ITERATIONS = "laterIterations";
const WHOLE_EXPANSION = "wholeExpansion";

// An empty mapped expansion is a placeholder at region index -1.
const isMappedTarget = (target: ClearTarget) => target.map_index >= 0 || target.region_index === -1;

// react/destructuring-assignment expects every prop access via signature
// destructure, but TypeScript's discriminated-union narrowing needs the union
// kept whole on the parameter — otherwise the `props.allMapped` discriminator
// is severed from `props.dagId` / `props.taskInstance`. Disable the rule for
// the parameter and the prop extraction; everything after uses local
// variables.
/* eslint-disable react/destructuring-assignment */
const ClearTaskInstanceDialog = (props: Props) => {
  const allMapped = props.allMapped === true;
  const allMappedTaskId = props.allMapped ? props.taskId : undefined;
  const taskInstance: TaskInstanceResponse | undefined =
    !props.allMapped && "taskInstance" in props ? props.taskInstance : undefined;
  const targets: Array<ClearTarget> = props.allMapped
    ? []
    : "taskInstances" in props
      ? props.taskInstances
      : [props.taskInstance];
  const groups: Array<RunGroup> = props.allMapped
    ? [{ dagId: props.dagId, dagRunId: props.dagRunId, targets: [] }]
    : getRunGroups(targets);
  const { onCleared } = props;
  const closeDialog = props.onClose;
  const openDialog = props.open;
  /* eslint-enable react/destructuring-assignment */
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();
  const isDialogOpen = openDialog && !open;

  // The Dag details and run versions behind the run-on-latest option are read for one run.
  const singleRun = groups.length === 1;
  const dagId = groups[0]?.dagId ?? "";
  const dagRunId = groups[0]?.dagRunId ?? "";
  const loopMode = targets.some((target) => target.in_loop);
  const initialNote = targets.length === 1 ? (targets[0]?.note ?? null) : null;

  const [clearTaskInstanceDefaultOptions] = useClearTaskInstanceDefaultOptions();
  const [preventRunningTaskDefault] = useClearPreventRunningTaskDefault();
  const [keepTaskStateDefault] = useClearKeepTaskStateDefault();
  const initialOptions = loopMode
    ? [...clearTaskInstanceDefaultOptions, LATER_ITERATIONS]
    : clearTaskInstanceDefaultOptions;
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(initialOptions);

  const onlyFailed = selectedOptions.includes("onlyFailed");
  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");
  const laterLoopIterations = selectedOptions.includes(LATER_ITERATIONS);
  const wholeExpansions = selectedOptions.includes(WHOLE_EXPANSION);
  const [keepTaskState, setKeepTaskState] = useState(keepTaskStateDefault);
  const [preventRunningTask, setPreventRunningTask] = useState(preventRunningTaskDefault);

  const [note, setNote] = useState<string | null>(initialNote);

  useEffect(() => {
    if (openDialog) {
      setNote(initialNote);
    }
  }, [openDialog, initialNote]);

  // Separate from the note effect above: this must only reset on open, not on every
  // note refetch, or a background note change while the dialog is open silently
  // discards the user's checked box.
  useEffect(() => {
    if (openDialog) {
      setKeepTaskState(keepTaskStateDefault);
    }
  }, [openDialog, keepTaskStateDefault]);

  const onCloseDialog = () => {
    setNote(initialNote);
    setKeepTaskState(keepTaskStateDefault);
    closeDialog();
  };

  // Get current DAG's bundle version to compare with task instance's DAG version bundle version
  const { data: dagDetails } = useDagServiceGetDagDetails({ dagId }, undefined, { enabled: singleRun });

  const { data: dagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId }, undefined, {
    enabled: openDialog && singleRun,
  });

  const runVersions = new Map((dagRun?.dag_versions ?? []).map((version) => [version.id, version]));
  const selectedVersions = targets.flatMap((target) => {
    const version =
      target.dag_version ??
      (target.dag_version_id === null || target.dag_version_id === undefined
        ? undefined
        : runVersions.get(target.dag_version_id));

    return version === undefined ? [] : [version];
  });
  const selectedVersion =
    selectedVersions.find(
      (version) => version.version_number !== dagDetails?.latest_dag_version?.version_number,
    ) ?? selectedVersions[0];

  const { dagVersionsDiffer, runOnLatestVersionForced, shouldShowRunOnLatestOption } =
    getRunOnLatestVersionState({
      latestBundleVersion: dagDetails?.bundle_version,
      latestDagVersionNumber: dagDetails?.latest_dag_version?.version_number,
      selectedBundleVersion: selectedVersion?.bundle_version,
      selectedDagVersionNumber: selectedVersion?.version_number,
      selectedVersionMissing: dagRun?.dag_versions.length === 0,
    });

  // dagVersionsDiffer becomes the fallback so the historical "auto-check when versions
  // differ" heuristic still applies when neither DAG-level nor global config is set.
  const { setValue: setRunOnLatestVersion, value: runOnLatestVersion } = useRerunWithLatestVersion({
    dagLevelConfig: dagDetails?.rerun_with_latest_version,
    fallback: dagVersionsDiffer,
  });
  const requestedRunOnLatestVersion = singleRun ? runOnLatestVersion : undefined;

  const { isPending, mutate } = useClearTaskInstances({
    dagRunId,
    onSuccessConfirm: () => {
      onCleared?.();
      onCloseDialog();
    },
  });

  const refetchInterval = useAutoRefresh({ dagId });

  // Loop members are addressed by exact execution id: their task id and map index alone
  // do not say which pass is meant. Past and future cannot be combined with exact ids.
  const previewRequests: Array<ClearRequest> = groups.map((group) => ({
    dagId: group.dagId,
    requestBody: loopMode
      ? {
          dag_run_id: group.dagRunId,
          include_downstream: downstream,
          include_later_loop_iterations: laterLoopIterations,
          include_upstream: upstream,
          only_failed: onlyFailed,
          run_on_latest_version: requestedRunOnLatestVersion,
          task_instance_ids: group.targets.map((target) => target.id),
          whole_expansion_ids: wholeExpansions
            ? group.targets.filter(isMappedTarget).map((target) => target.id)
            : [],
        }
      : {
          dag_run_id: group.dagRunId,
          include_downstream: downstream,
          include_future: future,
          include_past: past,
          include_upstream: upstream,
          only_failed: onlyFailed,
          run_on_latest_version: requestedRunOnLatestVersion,
          task_ids:
            allMappedTaskId === undefined
              ? group.targets.map((target): [string, number] => [target.task_id, target.map_index])
              : [allMappedTaskId],
        },
  }));

  const { data, error: dryRunError } = useClearTaskInstancesDryRuns({
    options: {
      enabled: openDialog,
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti: TaskInstanceResponse) => isStatePending(ti.state))
          ? refetchInterval
          : false,
      refetchOnMount: "always",
    },
    requests: previewRequests,
  });

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

  // Loop clears expand on the server (later iterations, whole expansions), so the
  // affected list cannot be turned back into an explicit selection there.
  const hasExclusions = !loopMode && excludedKeys.size > 0;
  const keptTaskInstances = data.task_instances.filter((ti) => !excludedKeys.has(taskInstanceKey(ti)));

  // The dry run already resolved the full affected set, so on confirm we send those
  // task instances explicitly (minus the unticked ones) with the graph-expansion flags
  // off, instead of re-deriving them from the selected task + upstream/downstream.
  // The clear endpoint only targets one run per request, so group the kept instances by
  // run and fire one run-scoped clear each. This honors per-run exclusions (e.g. keep task
  // X in run 1 but drop it from run 2) that a single flat request cannot express.
  const getKeptRequests = (): Array<ClearRequest> => {
    const idsByRun = new Map<string, { dagId: string; dagRunId: string; ids: Array<string> }>();

    for (const ti of keptTaskInstances) {
      const key = JSON.stringify([ti.dag_id, ti.dag_run_id]);
      const entry = idsByRun.get(key) ?? { dagId: ti.dag_id, dagRunId: ti.dag_run_id, ids: [] };

      entry.ids.push(ti.id);
      idsByRun.set(key, entry);
    }

    return [...idsByRun.values()].map(({ dagId: runDagId, dagRunId: runId, ids }) => ({
      dagId: runDagId,
      requestBody: {
        dag_run_id: runId,
        include_downstream: false,
        include_future: false,
        include_past: false,
        include_upstream: false,
        only_failed: onlyFailed,
        run_on_latest_version: requestedRunOnLatestVersion,
        task_instance_ids: ids,
      },
    }));
  };

  const confirmRequests = hasExclusions ? getKeptRequests() : previewRequests;
  const gateRequest = confirmRequests.length === 1 ? confirmRequests[0] : undefined;
  const confirmClear = () => {
    const noteChanged = note !== initialNote;

    for (const { dagId: requestDagId, requestBody } of confirmRequests) {
      mutate({
        dagId: requestDagId,
        requestBody: {
          ...requestBody,
          dry_run: false,
          note: noteChanged ? note : undefined,
          ...(keepTaskState ? { keep_task_state: true } : {}),
          ...(preventRunningTask ? { prevent_running_task: true } : {}),
        },
      });
    }
    onCloseDialog();
  };
  const hasNoLogicalDate =
    targets.length > 0 && targets.every((target) => (target.logical_date ?? null) === null);
  const hasMappedTargets = targets.some(isMappedTarget);

  return (
    <>
      <Modal
        footerActions={
          <>
            <Button
              disabled={data.total_entries === 0 || keptTaskInstances.length === 0}
              loading={isPending}
              onClick={gateRequest === undefined ? confirmClear : onOpen}
            >
              <CgRedo /> {translate("modal.confirm")}
            </Button>
            <Checkbox
              checked={keepTaskState}
              onCheckedChange={(event) => setKeepTaskState(Boolean(event.checked))}
              style={{ marginRight: "auto" }}
            >
              {translate("dags:runAndTaskActions.options.keepTaskState")}
            </Checkbox>
            <Checkbox
              checked={preventRunningTask}
              onCheckedChange={(event) => setPreventRunningTask(Boolean(event.checked))}
            >
              {translate("dags:runAndTaskActions.options.preventRunningTasks")}
            </Checkbox>
            {shouldShowRunOnLatestOption ? (
              <Checkbox
                checked={runOnLatestVersionForced || runOnLatestVersion}
                disabled={runOnLatestVersionForced}
                onCheckedChange={(event) => setRunOnLatestVersion(Boolean(event.checked))}
                title={
                  runOnLatestVersionForced
                    ? translate("dags:runAndTaskActions.options.runOnLatestVersionForced")
                    : undefined
                }
              >
                {translate("dags:runAndTaskActions.options.runOnLatestVersion")}
              </Checkbox>
            ) : undefined}
          </>
        }
        lazyMount
        onOpenChange={onCloseDialog}
        open={isDialogOpen}
        title={
          <>
            <strong>
              {allMapped
                ? translate("dags:runAndTaskActions.clearAllMapped.title")
                : translate("dags:runAndTaskActions.clear.title", {
                    type: translate(taskInstance === undefined ? "taskInstance_other" : "taskInstance_one"),
                  })}
              {allMapped || taskInstance !== undefined ? ":" : undefined}
            </strong>{" "}
            {allMappedTaskId ??
              (taskInstance === undefined ? undefined : (
                <>
                  {taskInstance.task_display_name} <Time datetime={taskInstance.start_date} />
                </>
              ))}
          </>
        }
      >
        <Flex justifyContent="center">
          <SegmentedControl
            defaultValues={initialOptions}
            multiple
            onChange={setSelectedOptions}
            options={[
              {
                disabled: allMapped || loopMode || hasNoLogicalDate,
                label: translate("dags:runAndTaskActions.options.past"),
                value: "past",
              },
              {
                disabled: allMapped || loopMode || hasNoLogicalDate,
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
              ...(loopMode
                ? [
                    {
                      label: translate("dags:runAndTaskActions.options.laterIterations"),
                      value: LATER_ITERATIONS,
                    },
                  ]
                : []),
              ...(loopMode && hasMappedTargets
                ? [
                    {
                      label: translate("dags:runAndTaskActions.options.wholeExpansion"),
                      value: WHOLE_EXPANSION,
                    },
                  ]
                : []),
            ]}
          />
        </Flex>
        <ErrorAlert error={dryRunError} />
        <ActionAccordion
          affectedTasks={data}
          groupByRunId={!singleRun}
          note={note}
          selection={loopMode ? undefined : { excludedKeys, onToggle: toggleTask }}
          setNote={setNote}
        />
      </Modal>
      {open && gateRequest !== undefined ? (
        <ClearTaskInstanceConfirmationDialog
          dryRun={gateRequest}
          onClose={onClose}
          onConfirm={confirmClear}
          open={open}
          preventRunningTask={preventRunningTask}
        />
      ) : null}
    </>
  );
};

export default ClearTaskInstanceDialog;
