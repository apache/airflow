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
import { Badge, Box, Button, Flex, Heading, HStack, VStack, useDisclosure } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiX } from "react-icons/fi";
import { LuCheck } from "react-icons/lu";

import type { TaskInstanceResponse, TaskInstanceState } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { ErrorAlert } from "src/components/ErrorAlert";
import { allowedStates } from "src/components/MarkAs/utils";
import { StateBadge } from "src/components/StateBadge";
import { Dialog, Menu } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useBulkMarkAsDryRun } from "src/queries/useBulkMarkAsDryRun";
import { useBulkTaskInstances } from "src/queries/useBulkTaskInstances";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedTaskInstances: Array<TaskInstanceResponse>;
};

const BulkMarkTaskInstancesAsButton = ({ clearSelections, selectedTaskInstances }: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();
  const [state, setState] = useState<TaskInstanceState>("success");
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>([]);
  const [note, setNote] = useState<string | null>(null);
  const { bulkAction, error, isPending, setError } = useBulkTaskInstances({
    clearSelections,
    onSuccessConfirm: onClose,
  });

  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");

  const hasLogicalDate = selectedTaskInstances.some((ti) => ti.logical_date !== null);

  const affectedCount = (targetState: TaskInstanceState) =>
    selectedTaskInstances.filter((ti) => ti.state !== targetState).length;

  const { data: affectedTasks, isFetching } = useBulkMarkAsDryRun(open, {
    options: {
      includeDownstream: downstream,
      includeFuture: future,
      includePast: past,
      includeUpstream: upstream,
    },
    selectedTaskInstances,
    targetState: state,
  });

  const handleOpen = (newState: TaskInstanceState) => {
    setState(newState);
    setSelectedOptions([]);
    setNote(null);
    setError(undefined);
    onOpen();
  };

  const directlyAffected = selectedTaskInstances.filter((ti) => ti.state !== state);

  return (
    <Box>
      <Menu.Root positioning={{ gutter: 0, placement: "top" }}>
        <Menu.Trigger asChild>
          <div>
            <Button colorPalette="brand" size="sm" variant="outline">
              <HStack gap={1} mx={1}>
                <LuCheck />
                <span>/</span>
                <FiX />
              </HStack>
              {translate("dags:runAndTaskActions.markAs.button", { type: translate("taskInstance_other") })}
            </Button>
          </div>
        </Menu.Trigger>
        <Menu.Content>
          {allowedStates.map((menuState) => {
            const count = affectedCount(menuState);

            return (
              <Menu.Item
                disabled={count === 0}
                key={menuState}
                onClick={() => {
                  if (count > 0) {
                    handleOpen(menuState);
                  }
                }}
                value={menuState}
              >
                <HStack justify="space-between" width="full">
                  <StateBadge state={menuState}>{translate(`common:states.${menuState}`)}</StateBadge>
                  <Badge colorPalette="gray" variant="subtle">
                    {count}
                  </Badge>
                </HStack>
              </Menu.Item>
            );
          })}
        </Menu.Content>
      </Menu.Root>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("dags:runAndTaskActions.markAs.title", {
                  state,
                  type: translate("taskInstance_other"),
                })}{" "}
                <StateBadge state={state} />
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />
          <Dialog.Body width="full">
            <Flex justifyContent="center" mb={4}>
              <SegmentedControl
                defaultValues={[]}
                multiple
                onChange={setSelectedOptions}
                options={[
                  {
                    disabled: !hasLogicalDate,
                    label: translate("dags:runAndTaskActions.options.past"),
                    value: "past",
                  },
                  {
                    disabled: !hasLogicalDate,
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
            <ActionAccordion affectedTasks={affectedTasks} groupByRunId note={note} setNote={setNote} />
            <ErrorAlert error={error} />
            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="brand"
                disabled={affectedTasks.total_entries === 0}
                loading={isPending || isFetching}
                onClick={() => {
                  bulkAction({
                    actions: [
                      {
                        action: "update" as const,
                        action_on_non_existence: "skip",
                        entities: directlyAffected.map((ti) => ({
                          dag_id: ti.dag_id,
                          dag_run_id: ti.dag_run_id,
                          include_downstream: downstream,
                          include_future: future,
                          include_past: past,
                          include_upstream: upstream,
                          map_index: ti.map_index,
                          new_state: state,
                          note,
                          task_id: ti.task_id,
                        })),
                        update_mask: note === null ? ["new_state"] : ["new_state", "note"],
                      },
                    ],
                  });
                }}
              >
                {translate("modal.confirm")}
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};

export default BulkMarkTaskInstancesAsButton;
