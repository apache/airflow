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
import { Flex, Heading, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import { useDagServiceGetDagDetails } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import Time from "src/components/Time";
import { Button, Dialog, Checkbox } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";
import { usePatchTaskInstance } from "src/queries/usePatchTaskInstance";
import { isStatePending, useAutoRefresh } from "src/utils";

type Props = {
  readonly onClose: () => void;
  readonly open: boolean;
  readonly taskInstance: TaskInstanceResponse;
};

const ClearTaskInstanceDialog = ({ onClose, open, taskInstance }: Props) => {
  const taskId = taskInstance.task_id;
  const mapIndex = taskInstance.map_index;
  const { t: translate } = useTranslation();

  const dagId = taskInstance.dag_id;
  const dagRunId = taskInstance.dag_run_id;

  const { isPending, mutate } = useClearTaskInstances({
    dagId,
    dagRunId,
    onSuccessConfirm: onClose,
  });

  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["downstream"]);

  const onlyFailed = selectedOptions.includes("onlyFailed");
  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");
  const [runOnLatestVersion, setRunOnLatestVersion] = useState(false);

  const [note, setNote] = useState<string | null>(taskInstance.note);
  const { isPending: isPendingPatchDagRun, mutate: mutatePatchTaskInstance } = usePatchTaskInstance({
    dagId,
    dagRunId,
    mapIndex,
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
      enabled: open,
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
      task_ids: [[taskId, mapIndex]],
    },
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  // Check if bundle versions are different
  const currentDagBundleVersion = dagDetails?.bundle_version;
  const taskInstanceDagVersionBundleVersion = taskInstance.dag_version?.bundle_version;
  const bundleVersionsDiffer = currentDagBundleVersion !== taskInstanceDagVersionBundleVersion;
  const shouldShowBundleVersionOption =
    bundleVersionsDiffer &&
    taskInstanceDagVersionBundleVersion !== null &&
    taskInstanceDagVersionBundleVersion !== "";

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              <strong>
                {translate("dags:runAndTaskActions.clear.title", {
                  type: translate("taskInstance_one"),
                })}
                :
              </strong>{" "}
              {taskInstance.task_display_name} <Time datetime={taskInstance.start_date} />
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
                  disabled: taskInstance.logical_date === null,
                  label: translate("dags:runAndTaskActions.options.past"),
                  value: "past",
                },
                {
                  disabled: taskInstance.logical_date === null,
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
            <Button
              colorPalette="brand"
              disabled={affectedTasks.total_entries === 0}
              loading={isPending || isPendingPatchDagRun}
              onClick={() => {
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
                    task_ids: [[taskId, mapIndex]],
                  },
                });
                if (note !== taskInstance.note) {
                  mutatePatchTaskInstance({
                    dagId,
                    dagRunId,
                    mapIndex,
                    requestBody: { note },
                    taskId,
                  });
                }
              }}
            >
              <CgRedo /> {translate("modal.confirm")}
            </Button>
          </Flex>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default ClearTaskInstanceDialog;
