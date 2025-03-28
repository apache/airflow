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

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Button, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { usePatchTaskInstance } from "src/queries/usePatchTaskInstance";
import { usePatchTaskInstanceDryRun } from "src/queries/usePatchTaskInstanceDryRun";

import type { TaskActionProps } from "../utils";

type Props = {
  readonly onClose: () => void;
  readonly open: boolean;
  readonly state: TaskInstanceState;
  readonly taskActionProps: TaskActionProps;
};

const MarkTaskInstanceAsDialog = ({
  onClose,
  open,
  state,
  taskActionProps: {
    dagId,
    dagRunId,
    logicalDate,
    mapIndex = -1,
    note: taskNote,
    startDate,
    taskDisplayName,
    taskId,
  },
}: Props) => {
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>([]);

  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");

  // eslint-disable-next-line unicorn/no-null
  const [note, setNote] = useState<string | null>(taskNote ?? null);

  const { isPending, mutate } = usePatchTaskInstance({
    dagId,
    dagRunId,
    mapIndex,
    onSuccess: onClose,
    taskId,
  });
  const { data, isPending: isPendingDryRun } = usePatchTaskInstanceDryRun({
    dagId,
    dagRunId,
    mapIndex,
    options: {
      enabled: open,
    },
    requestBody: {
      include_downstream: downstream,
      include_future: future,
      include_past: past,
      include_upstream: upstream,
      new_state: state,
      note,
    },
    taskId,
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              <strong>Mark Task Instance as {state}:</strong> {taskDisplayName ?? taskId}{" "}
              <Time datetime={startDate} /> <StateBadge state={state} />
            </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body width="full">
          <Flex justifyContent="center">
            <SegmentedControl
              multiple
              onChange={setSelectedOptions}
              options={[
                { disabled: !Boolean(logicalDate), label: "Past", value: "past" },
                { disabled: !Boolean(logicalDate), label: "Future", value: "future" },
                { label: "Upstream", value: "upstream" },
                { label: "Downstream", value: "downstream" },
              ]}
            />
          </Flex>
          <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
          <Flex justifyContent="end" mt={3}>
            <Button
              colorPalette="blue"
              loading={isPending || isPendingDryRun}
              onClick={() => {
                mutate({
                  dagId,
                  dagRunId,
                  mapIndex,
                  requestBody: {
                    include_downstream: downstream,
                    include_future: future,
                    include_past: past,
                    include_upstream: upstream,
                    new_state: state,
                    note,
                  },
                  taskId,
                });
              }}
            >
              Confirm
            </Button>
          </Flex>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default MarkTaskInstanceAsDialog;
