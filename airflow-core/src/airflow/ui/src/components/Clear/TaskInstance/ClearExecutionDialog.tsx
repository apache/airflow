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

import { Button, Stack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useDagRunServiceGetDagRun, useDagServiceGetDagDetails } from "openapi/queries";
import type { ClearTaskInstancesBody, ExecutionTaskResponse } from "openapi/requests/types.gen";

import { Checkbox, Modal } from "src/system-components";

import { ActionAccordion } from "src/components/ActionAccordion";
import { useRerunWithLatestVersion } from "src/components/Clear/useRerunWithLatestVersion";
import { ErrorAlert } from "src/components/ErrorAlert";

import {
  useClearKeepTaskStateDefault,
  useClearPreventRunningTaskDefault,
  useClearTaskInstanceDefaultOptions,
} from "src/hooks/useUserSettings";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";

import { getRunOnLatestVersionState } from "./runOnLatestVersion";

type SelectedExecution = Partial<Pick<ExecutionTaskResponse, "dag_version_id">> &
  Pick<
    ExecutionTaskResponse,
    "id" | "map_index" | "note" | "region_id" | "region_index" | "task_display_name" | "task_id"
  >;

export const ClearExecutionDialog = ({
  dagId,
  executions,
  onCleared,
  onClose,
  open,
  runId,
}: {
  readonly dagId: string;
  readonly executions: Array<SelectedExecution>;
  readonly onCleared?: () => void;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly runId: string;
}) => {
  const { t: translate } = useTranslation("dag");
  const [defaultOptions] = useClearTaskInstanceDefaultOptions();
  const [preventRunningDefault] = useClearPreventRunningTaskDefault();
  const [keepTaskStateDefault] = useClearKeepTaskStateDefault();
  const initialNote = (executions.length === 1 ? executions[0]?.note : undefined) ?? "";
  const defaultDownstream = defaultOptions.includes("downstream");
  const [downstream, setDownstream] = useState(defaultDownstream);
  const [later, setLater] = useState(true);
  const [whole, setWhole] = useState(false);
  const [preventRunning, setPreventRunning] = useState(preventRunningDefault);
  const [keepTaskState, setKeepTaskState] = useState(keepTaskStateDefault);
  const [note, setNote] = useState(initialNote);

  useEffect(() => {
    if (!open) {
      setDownstream(defaultDownstream);
      setLater(true);
      setWhole(false);
      setPreventRunning(preventRunningDefault);
      setKeepTaskState(keepTaskStateDefault);
      setNote(initialNote);
    }
  }, [open, defaultDownstream, preventRunningDefault, keepTaskStateDefault, initialNote]);
  const { data: dagDetails } = useDagServiceGetDagDetails({ dagId }, undefined, { enabled: open });
  const { data: dagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId }, undefined, {
    enabled: open,
  });
  const runVersions = new Map((dagRun?.dag_versions ?? []).map((version) => [version.id, version]));
  const selectedVersions = executions.flatMap((ti) => {
    const version =
      ti.dag_version_id === null || ti.dag_version_id === undefined
        ? undefined
        : runVersions.get(ti.dag_version_id);

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
  const { setValue: setRunOnLatestVersion, value: runOnLatestVersion } = useRerunWithLatestVersion({
    dagLevelConfig: dagDetails?.rerun_with_latest_version,
    fallback: dagVersionsDiffer,
  });
  const mappedIds = executions.filter((ti) => ti.map_index >= 0 || ti.region_index === -1).map((ti) => ti.id);
  const requestBody: ClearTaskInstancesBody = {
    dag_run_id: runId,
    include_downstream: downstream,
    include_later_loop_iterations: later,
    keep_task_state: keepTaskState,
    only_failed: false,
    prevent_running_task: preventRunning,
    run_on_latest_version: runOnLatestVersion,
    task_instance_ids: executions.map((ti) => ti.id),
    whole_expansion_ids: whole ? mappedIds : [],
  };
  const preview = useClearTaskInstancesDryRun({
    dagId,
    options: { enabled: open, retry: false },
    requestBody,
  });
  const clear = useClearTaskInstances({
    dagId,
    dagRunId: runId,
    onSuccessConfirm: () => {
      onCleared?.();
      onClose();
    },
  });

  return (
    <Modal
      footerActions={
        <Button
          disabled={preview.isPending || preview.isError}
          loading={clear.isPending}
          onClick={() =>
            clear.mutate({
              dagId,
              requestBody: {
                ...requestBody,
                dry_run: false,
                note: note === initialNote ? undefined : note || undefined,
              },
            })
          }
        >
          {translate("execution.clearSelected")}
        </Button>
      }
      onOpenChange={(details) => {
        if (!details.open) {
          onClose();
        }
      }}
      open={open}
      title={translate("execution.clearSelected")}
    >
      <Stack gap={4}>
        <ErrorAlert error={preview.error} />
        <Checkbox checked={downstream} onCheckedChange={(details) => setDownstream(details.checked === true)}>
          {translate("execution.clearDownstream")}
        </Checkbox>
        <Checkbox checked={later} onCheckedChange={(details) => setLater(details.checked === true)}>
          {translate("execution.clearLater")}
        </Checkbox>
        {mappedIds.length > 0 ? (
          <Checkbox checked={whole} onCheckedChange={(details) => setWhole(details.checked === true)}>
            {translate("execution.clearWhole")}
          </Checkbox>
        ) : undefined}
        <Checkbox
          checked={preventRunning}
          onCheckedChange={(details) => setPreventRunning(details.checked === true)}
        >
          {translate("dags:runAndTaskActions.options.preventRunningTasks")}
        </Checkbox>
        <Checkbox
          checked={keepTaskState}
          onCheckedChange={(details) => setKeepTaskState(details.checked === true)}
        >
          {translate("dags:runAndTaskActions.options.keepTaskState")}
        </Checkbox>
        {shouldShowRunOnLatestOption ? (
          <Checkbox
            checked={runOnLatestVersionForced || runOnLatestVersion}
            disabled={runOnLatestVersionForced}
            onCheckedChange={(details) => setRunOnLatestVersion(details.checked === true)}
            title={
              runOnLatestVersionForced
                ? translate("dags:runAndTaskActions.options.runOnLatestVersionForced")
                : undefined
            }
          >
            {translate("dags:runAndTaskActions.options.runOnLatestVersion")}
          </Checkbox>
        ) : undefined}
        <ActionAccordion affectedTasks={preview.data} note={note} setNote={setNote} />
      </Stack>
    </Modal>
  );
};
