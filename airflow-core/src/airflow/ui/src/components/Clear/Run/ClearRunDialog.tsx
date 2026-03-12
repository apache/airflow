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
import { Button, Flex, Heading, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import { useDagServiceGetDagDetails } from "openapi/queries";
import type {
  DAGRunResponse,
  NewTaskCollectionResponse,
  TaskInstanceCollectionResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { Checkbox, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useClearDagRunDryRun } from "src/queries/useClearDagRunDryRun";
import { useClearDagRun } from "src/queries/useClearRun";
import { usePatchDagRun } from "src/queries/usePatchDagRun";
import { isStatePending, useAutoRefresh } from "src/utils";

/** Type guard to distinguish NewTaskCollectionResponse from TaskInstanceCollectionResponse. */
const isNewTaskCollection = (
  data: NewTaskCollectionResponse | TaskInstanceCollectionResponse,
): data is NewTaskCollectionResponse => "new_tasks" in data;

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly onClose: () => void;
  readonly open: boolean;
};

const ClearRunDialog = ({ dagRun, onClose, open }: Props) => {
  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;
  const { t: translate } = useTranslation();

  const [note, setNote] = useState<string | null>(dagRun.note);
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["existingTasks"]);
  const onlyFailed = selectedOptions.includes("onlyFailed");
  const onlyNew = selectedOptions.includes("new_tasks");
  const [runOnLatestVersion, setRunOnLatestVersion] = useState(false);

  // Get current DAG's bundle version to compare with DAG run's bundle version
  const { data: dagDetails } = useDagServiceGetDagDetails({
    dagId,
  });

  const refetchInterval = useAutoRefresh({ dagId });

  const { data: dryRunData } = useClearDagRunDryRun({
    dagId,
    dagRunId,
    options: {
      refetchInterval: (query) => {
        const queryData = query.state.data;

        if (!queryData || isNewTaskCollection(queryData)) {
          return false;
        }

        return queryData.task_instances.some((ti: TaskInstanceResponse) => isStatePending(ti.state))
          ? refetchInterval
          : false;
      },
    },
    requestBody: {
      only_failed: onlyFailed,
      only_new: onlyNew,
      run_on_latest_version: runOnLatestVersion,
    },
  });

  // Normalise both response shapes into the format ActionAccordion expects.
  const affectedTasks: TaskInstanceCollectionResponse = (() => {
    const empty: TaskInstanceCollectionResponse = { task_instances: [], total_entries: 0 };

    if (!dryRunData) {
      return empty;
    }
    if (isNewTaskCollection(dryRunData)) {
      return {
        task_instances: dryRunData.new_tasks.map(
          (task) => ({ task_id: task.task_id }) as TaskInstanceResponse,
        ),
        total_entries: dryRunData.total_entries,
      };
    }

    return dryRunData;
  })();

  const { isPending, mutate } = useClearDagRun({
    dagId,
    dagRunId,
    onSuccessConfirm: onClose,
  });

  const { isPending: isPendingPatchDagRun, mutate: mutatePatchDagRun } = usePatchDagRun({
    dagId,
    dagRunId,
    onSuccess: onClose,
  });

  // Check if DAG versions differ (works for both bundle-versioned and local bundles)
  const latestDagVersionNumber = dagDetails?.latest_dag_version?.version_number;
  const dagRunVersionNumber = dagRun.dag_versions.at(-1)?.version_number;
  const versionsDiffer =
    latestDagVersionNumber !== undefined &&
    dagRunVersionNumber !== undefined &&
    latestDagVersionNumber !== dagRunVersionNumber;
  const shouldShowBundleVersionOption = versionsDiffer && !onlyNew;

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              <strong>
                {translate("dags:runAndTaskActions.clear.title", { type: translate("dagRun_one") })}:{" "}
              </strong>{" "}
              {dagRunId}
            </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body width="full">
          <Flex justifyContent="center">
            <SegmentedControl
              defaultValues={["existingTasks"]}
              onChange={setSelectedOptions}
              options={[
                {
                  label: translate("dags:runAndTaskActions.options.existingTasks"),
                  value: "existingTasks",
                },
                {
                  label: translate("dags:runAndTaskActions.options.onlyFailed"),
                  value: "onlyFailed",
                },
                {
                  label: translate("dags:runAndTaskActions.options.queueNew"),
                  value: "new_tasks",
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
                  dagRunId,
                  requestBody: {
                    dry_run: false,
                    only_failed: onlyFailed,
                    only_new: onlyNew,
                    run_on_latest_version: runOnLatestVersion,
                  },
                });
                if (note !== dagRun.note) {
                  mutatePatchDagRun({
                    dagId,
                    dagRunId,
                    requestBody: { note },
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

export default ClearRunDialog;
