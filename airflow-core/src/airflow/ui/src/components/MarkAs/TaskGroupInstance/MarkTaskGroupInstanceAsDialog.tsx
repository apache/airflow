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
  LightGridTaskInstanceSummary,
  TaskInstanceState,
  BulkBody_BulkTaskInstanceBody_,
} from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";
import { Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useBulkTaskInstances } from "src/queries/useBulkTaskInstances";
import { useBulkTaskInstancesDryRun } from "src/queries/useBulkTaskInstancesDryRun";

type Props = {
  readonly groupTaskInstance: LightGridTaskInstanceSummary;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly state: TaskInstanceState;
};

const MarkTaskGroupInstanceAsDialog = ({ groupTaskInstance, onClose, open, state }: Props) => {
  const { t: translate } = useTranslation();
  const { dagId = "", runId = "" } = useParams();
  const groupId = groupTaskInstance.task_id;

  const [selectedOptions, setSelectedOptions] = useState<Array<string>>([]);

  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");

  const [note, setNote] = useState<string>("");

  // Get all task instances in the group
  const { data: groupTaskInstances } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId,
      dagRunId: runId,
      taskDisplayNamePattern: groupId,
    },
    undefined,
    {
      enabled: open,
    },
  );

  const groupTaskIds = groupTaskInstances?.task_instances.map((ti) => ti.task_id) ?? [];

  // Build bulk update request
  const { isPending, mutate } = useBulkTaskInstances({
    dagId,
    onSuccess: onClose,
    runId,
  });
  const { data, isPending: isPendingDryRun } = useBulkTaskInstancesDryRun({
    dagId,
    dagRunId: runId,
    options: {
      enabled: open,
      refetchOnMount: "always",
    },
    requestBody: {
      actions: [
        {
          action: "update",
          entities: groupTaskIds.map((taskId) => ({
            include_downstream: downstream,
            include_future: future,
            include_past: past,
            include_upstream: upstream,
            map_index: undefined,
            new_state: state,
            note: note || undefined,
            task_id: taskId,
          })),
        },
      ],
    },
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  const handleConfirm = () => {
    const bulkBody: BulkBody_BulkTaskInstanceBody_ = {
      actions: [
        {
          action: "update",
          entities: groupTaskIds.map((taskId) => ({
            include_downstream: downstream,
            include_future: future,
            include_past: past,
            include_upstream: upstream,
            map_index: undefined,
            new_state: state,
            note: note || undefined,
            task_id: taskId,
          })),
        },
      ],
    };

    mutate({
      dagId,
      dagRunId: runId,
      requestBody: bulkBody,
    });
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl">
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
              {groupId} <StateBadge state={state} />
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
              ]}
            />
          </Flex>
          <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
          <Flex justifyContent="end" mt={3}>
            <Button
              colorPalette="brand"
              disabled={affectedTasks.total_entries === 0}
              loading={isPending || isPendingDryRun}
              onClick={handleConfirm}
            >
              {translate("modal.confirm")}
            </Button>
          </Flex>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default MarkTaskGroupInstanceAsDialog;
