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
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails, useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { Button, Dialog, Checkbox } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";

type Props = {
  readonly onClose: () => void;
  readonly open: boolean;
  readonly taskInstance: LightGridTaskInstanceSummary;
};

export const ClearGroupTaskInstanceDialog = ({ onClose, open, taskInstance }: Props) => {
  const { t: translate } = useTranslation();
  const { dagId = "", runId = "" } = useParams();
  const groupId = taskInstance.task_id;

  const { isPending, mutate } = useClearTaskInstances({
    dagId,
    dagRunId: runId,
    onSuccessConfirm: onClose,
  });

  const [selectedOptions, setSelectedOptions] = useState<Array<string>>([]);

  const onlyFailed = selectedOptions.includes("onlyFailed");
  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");
  const [runOnLatestVersion, setRunOnLatestVersion] = useState(false);

  const [note, setNote] = useState<string>("");

  const { data: dagDetails } = useDagServiceGetDagDetails({
    dagId,
  });

  const { data: groupTaskInstances } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId,
      dagRunId: runId,
      taskGroup: groupId,
    },
    undefined,
    {
      enabled: open,
    },
  );

  const groupTaskIds = groupTaskInstances?.task_instances.map((ti) => ti.task_id) ?? [];

  const { data } = useClearTaskInstancesDryRun({
    dagId,
    options: {
      enabled: open && groupTaskIds.length > 0,
      refetchOnMount: "always",
    },
    requestBody: {
      dag_run_id: runId,
      include_downstream: downstream,
      include_future: future,
      include_past: past,
      include_upstream: upstream,
      only_failed: onlyFailed,
      run_on_latest_version: runOnLatestVersion,
      task_ids: groupTaskIds,
    },
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  const shouldShowBundleVersionOption =
    dagDetails?.bundle_version !== null && dagDetails?.bundle_version !== "";

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              <strong>
                {translate("dags:runAndTaskActions.clear.title", {
                  type: translate("taskInstance", { count: affectedTasks.total_entries }),
                })}
                :
              </strong>{" "}
              {groupId}
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
              disabled={affectedTasks.total_entries === 0 || groupTaskIds.length === 0}
              loading={isPending}
              onClick={() => {
                mutate({
                  dagId,
                  requestBody: {
                    dag_run_id: runId,
                    dry_run: false,
                    include_downstream: downstream,
                    include_future: future,
                    include_past: past,
                    include_upstream: upstream,
                    only_failed: onlyFailed,
                    run_on_latest_version: runOnLatestVersion,
                    task_ids: groupTaskIds,
                  },
                });
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
