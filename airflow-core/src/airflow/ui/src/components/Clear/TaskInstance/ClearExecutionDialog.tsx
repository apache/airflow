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

import { Button, Stack, Text, Textarea } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { ClearTaskInstancesBody, ExecutionTaskResponse } from "openapi/requests/types.gen";

import { Checkbox, Modal } from "src/system-components";

import { ErrorAlert } from "src/components/ErrorAlert";

import {
  useClearKeepTaskStateDefault,
  useClearPreventRunningTaskDefault,
  useClearTaskInstanceDefaultOptions,
} from "src/hooks/useUserSettings";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";

type SelectedExecution = Pick<
  ExecutionTaskResponse,
  "id" | "map_index" | "note" | "region_id" | "region_index" | "task_display_name" | "task_id"
>;

export const ClearExecutionDialog = ({
  dagId,
  executions,
  onClose,
  open,
  runId,
}: {
  readonly dagId: string;
  readonly executions: Array<SelectedExecution>;
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
  const mappedIds = executions
    .filter(
      (ti) =>
        ti.map_index >= 0 ||
        (ti.region_id !== "00000000-0000-0000-0000-000000000000" && ti.region_index === -1),
    )
    .map((ti) => ti.id);
  const requestBody: ClearTaskInstancesBody = {
    dag_run_id: runId,
    include_downstream: downstream,
    include_later_loop_iterations: later,
    keep_task_state: keepTaskState,
    only_failed: false,
    prevent_running_task: preventRunning,
    task_instance_ids: executions.map((ti) => ti.id),
    whole_expansion_ids: whole ? mappedIds : [],
  };
  const preview = useClearTaskInstancesDryRun({
    dagId,
    options: { enabled: open, retry: false },
    requestBody,
  });
  const clear = useClearTaskInstances({ dagId, dagRunId: runId, onSuccessConfirm: onClose });

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
        {preview.data === undefined ? undefined : (
          <Text>{translate("execution.clearAffected", { count: preview.data.total_entries })}</Text>
        )}
        <Textarea
          aria-label={translate("execution.clearNote")}
          maxLength={1000}
          onChange={(event) => setNote(event.target.value)}
          placeholder={translate("execution.clearNote")}
          value={note}
        />
      </Stack>
    </Modal>
  );
};
