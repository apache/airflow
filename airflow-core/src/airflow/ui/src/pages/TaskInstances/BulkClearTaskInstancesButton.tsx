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
import { Button, Flex, Heading, VStack, useDisclosure } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Checkbox, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useBulkClearDryRun } from "src/queries/useBulkClearDryRun";
import { useBulkClearTaskInstances } from "src/queries/useBulkClearTaskInstances";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedTaskInstances: Array<TaskInstanceResponse>;
};

const BulkClearTaskInstancesButton = ({ clearSelections, selectedTaskInstances }: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["downstream"]);
  const [note, setNote] = useState<string | null>(null);
  const [preventRunningTask, setPreventRunningTask] = useState(true);
  const { bulkClear, error, isPending } = useBulkClearTaskInstances({
    clearSelections,
    onSuccessConfirm: onClose,
  });

  const handleClose = () => {
    setNote(null);
    onClose();
  };

  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");
  const onlyFailed = selectedOptions.includes("onlyFailed");

  const hasLogicalDate = selectedTaskInstances.some((ti) => ti.logical_date !== null);

  const { data: affectedTasks, isFetching } = useBulkClearDryRun(open, selectedTaskInstances, {
    includeDownstream: downstream,
    includeFuture: future,
    includeOnlyFailed: onlyFailed,
    includePast: past,
    includeUpstream: upstream,
  });

  return (
    <>
      <Button colorPalette="brand" onClick={onOpen} size="sm" variant="outline">
        <CgRedo />
        {translate("dags:runAndTaskActions.clear.button", { type: translate("taskInstance_other") })}
      </Button>

      <Dialog.Root onOpenChange={handleClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("dags:runAndTaskActions.clear.title", {
                  type: translate("taskInstance_other"),
                })}
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />
          <Dialog.Body width="full">
            <Flex justifyContent="center" mb={4}>
              <SegmentedControl
                defaultValues={["downstream"]}
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
                  {
                    label: translate("dags:runAndTaskActions.options.onlyFailed"),
                    value: "onlyFailed",
                  },
                ]}
              />
            </Flex>
            <ActionAccordion affectedTasks={affectedTasks} groupByRunId note={note} setNote={setNote} />
            <ErrorAlert error={error} />
            <Flex alignItems="center" justifyContent="space-between" mt={3}>
              <Checkbox
                checked={preventRunningTask}
                onCheckedChange={(event) => setPreventRunningTask(Boolean(event.checked))}
              >
                {translate("dags:runAndTaskActions.options.preventRunningTasks")}
              </Checkbox>
              <Button
                colorPalette="brand"
                disabled={affectedTasks.total_entries === 0}
                loading={isPending || isFetching}
                onClick={() => {
                  void bulkClear(selectedTaskInstances, {
                    includeDownstream: downstream,
                    includeFuture: future,
                    includeOnlyFailed: onlyFailed,
                    includePast: past,
                    includeUpstream: upstream,
                    note,
                    preventRunningTask,
                  });
                }}
              >
                <CgRedo />
                {translate("modal.confirm")}
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default BulkClearTaskInstancesButton;
