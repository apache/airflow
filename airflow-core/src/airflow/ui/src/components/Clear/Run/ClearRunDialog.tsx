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
import { CgRedo } from "react-icons/cg";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { Button, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useClearDagRunDryRun } from "src/queries/useClearDagRunDryRun";
import { useClearDagRun } from "src/queries/useClearRun";
import { usePatchDagRun } from "src/queries/usePatchDagRun";

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly onClose: () => void;
  readonly open: boolean;
};

const ClearRunDialog = ({ dagRun, onClose, open }: Props) => {
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>([]);

  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;

  const { isPending, mutate } = useClearDagRun({
    dagId,
    dagRunId,
    onSuccessConfirm: onClose,
  });

  const onlyFailed = selectedOptions.includes("onlyFailed");

  const [note, setNote] = useState<string | null>(dagRun.note);
  const { isPending: isPendingPatchDagRun, mutate: mutatePatchDagRun } = usePatchDagRun({ dagId, dagRunId });

  const { data } = useClearDagRunDryRun({
    dagId,
    dagRunId,
    options: {
      enabled: open,
      refetchOnMount: "always",
    },
    requestBody: {
      only_failed: onlyFailed,
    },
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
              <strong>Clear DagRun: </strong> {dagRunId}
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
                { label: "Clear existing tasks", value: "existingTasks" },
                { label: "Clear only failed tasks", value: "onlyFailed" },
                {
                  disabled: true,
                  label: "Queue up new tasks",
                  value: "new_tasks",
                },
              ]}
            />
          </Flex>
          <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
          <Flex justifyContent="end" mt={3}>
            <Button
              colorPalette="blue"
              disabled={affectedTasks.total_entries === 0}
              loading={isPending || isPendingPatchDagRun}
              onClick={() => {
                mutate({
                  dagId,
                  dagRunId,
                  requestBody: { dry_run: false, only_failed: onlyFailed },
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
              <CgRedo /> Confirm
            </Button>
          </Flex>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default ClearRunDialog;
