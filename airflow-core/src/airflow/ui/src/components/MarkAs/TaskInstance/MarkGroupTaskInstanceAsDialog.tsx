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
import { useParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import type {
  BulkBody_BulkTaskInstanceBody_,
  BulkUpdateAction_BulkTaskInstanceBody_,
  BulkTaskInstanceBody,
  LightGridTaskInstanceSummary,
  TaskInstanceState,
} from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useBulkUpdateTaskInstances } from "src/queries/useBulkUpdateTaskInstances";
import { useBulkUpdateTaskInstancesDryRun } from "src/queries/useBulkUpdateTaskInstancesDryRun";

type Props = {
  readonly groupTaskInstance: LightGridTaskInstanceSummary;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly state: TaskInstanceState;
};

const MarkGroupTaskInstanceAsDialog = ({ groupTaskInstance, onClose, open, state }: Props) => {
  const { t: translate } = useTranslation();
  const { dagId = "", runId = "" } = useParams();
  const groupId = groupTaskInstance.task_id;

  const [selectedOptions, setSelectedOptions] = useState<Array<string>>([]);

  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");

  const [note, setNote] = useState<string | undefined>(undefined);

  const { data: groupTaskInstances } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId,
      dagRunId: runId,
      taskGroupId: groupId,
    },
    undefined,
    {
      enabled: open,
    },
  );

  const groupTaskIds = groupTaskInstances?.task_instances.map((ti) => ti.task_id) ?? [];

  // Create bulk update action with all tasks in the group for dry run
  const bulkBodyForDryRun: BulkBody_BulkTaskInstanceBody_ | undefined =
    groupTaskInstances && groupTaskIds.length > 0
      ? {
          actions: [
            {
              action: "update",
              entities: groupTaskInstances.task_instances.map((ti) => ({
                include_downstream: downstream,
                include_future: future,
                include_past: past,
                include_upstream: upstream,
                map_index: ti.map_index,
                new_state: state,
                note,
                task_id: ti.task_id,
              })),
            },
          ],
        }
      : undefined;

  const { data, isPending: isPendingDryRun } = useBulkUpdateTaskInstancesDryRun({
    dagId,
    dagRunId: runId,
    options: {
      enabled: open && groupTaskIds.length > 0,
      refetchOnMount: "always",
    },
    requestBody: bulkBodyForDryRun,
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  const { isPending, mutateAsync } = useBulkUpdateTaskInstances({
    affectsMultipleRuns: future || past,
    dagId,
    dagRunId: runId,
    onSuccess: onClose,
  });

  const handleConfirm = async () => {
    if (groupTaskIds.length === 0 || !groupTaskInstances) {
      return;
    }

    // Create bulk update action with all tasks in the group
    const entities: Array<BulkTaskInstanceBody> = groupTaskInstances.task_instances.map((ti) => ({
      include_downstream: downstream,
      include_future: future,
      include_past: past,
      include_upstream: upstream,
      map_index: ti.map_index,
      new_state: state,
      note,
      task_id: ti.task_id,
    }));

    const updateAction: BulkUpdateAction_BulkTaskInstanceBody_ = {
      action: "update",
      entities,
    };

    const bulkBody: BulkBody_BulkTaskInstanceBody_ = {
      actions: [updateAction],
    };

    await mutateAsync({
      dagId,
      dagRunId: runId,
      requestBody: bulkBody,
    });
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl" unmountOnExit>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              <strong>
                {translate("dags:runAndTaskActions.markAs.title", {
                  state,
                  type: translate("taskGroup"),
                })}
                :
              </strong>{" "}
              {groupId} <Time datetime={groupTaskInstance.min_start_date} /> <StateBadge state={state} />
            </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body width="full">
          <Flex justifyContent="center">
            <SegmentedControl
              defaultValues={[]}
              multiple
              onChange={setSelectedOptions}
              options={[
                {
                  disabled: groupTaskInstance.min_start_date === null,
                  label: translate("dags:runAndTaskActions.options.past"),
                  value: "past",
                },
                {
                  disabled: groupTaskInstance.min_start_date === null,
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
              ]}
            />
          </Flex>
          <ActionAccordion affectedTasks={affectedTasks} note={note ?? null} setNote={setNote} />
          <Flex justifyContent="end" mt={3}>
            <Button
              colorPalette="brand"
              disabled={groupTaskIds.length === 0}
              loading={isPending || isPendingDryRun}
              onClick={() => {
                void handleConfirm();
              }}
            >
              {translate("modal.confirm")}
            </Button>
          </Flex>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default MarkGroupTaskInstanceAsDialog;
