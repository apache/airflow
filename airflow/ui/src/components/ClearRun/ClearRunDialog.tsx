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
import { FiRefreshCw } from "react-icons/fi";

import type {
  DAGRunClearBody,
  DAGRunResponse,
  TaskInstanceCollectionResponse,
} from "openapi/requests/types.gen";
import { Button, Dialog } from "src/components/ui";
import { usePatchDagRun } from "src/queries/usePatchDagRun";

import SegmentedControl from "../ui/SegmentedControl";
import ClearRunTasksAccordion from "./ClearRunTaskAccordion";

type Props = {
  readonly affectedTasks: TaskInstanceCollectionResponse;
  readonly dagRun: DAGRunResponse;
  readonly isPending: boolean;
  readonly mutate: ({
    dagId,
    dagRunId,
    requestBody,
  }: {
    dagId: string;
    dagRunId: string;
    requestBody: DAGRunClearBody;
  }) => void;
  readonly onClose: () => void;
  readonly onlyFailed: boolean;
  readonly open: boolean;
  readonly setOnlyFailed: (value: boolean) => void;
};

const ClearRunDialog = ({
  affectedTasks,
  dagRun,
  isPending,
  mutate,
  onClose,
  onlyFailed,
  open,
  setOnlyFailed,
}: Props) => {
  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;

  const [note, setNote] = useState<string | null>(dagRun.note);
  const { isPending: isPendingPatchDagRun, mutate: mutatePatchDagRun } = usePatchDagRun({ dagId, dagRunId });

  const onChange = (value: string) => {
    switch (value) {
      case "existing_tasks":
        setOnlyFailed(false);
        mutate({
          dagId,
          dagRunId,
          requestBody: { dry_run: true, only_failed: false },
        });
        break;
      case "only_failed":
        setOnlyFailed(true);
        mutate({
          dagId,
          dagRunId,
          requestBody: { dry_run: true, only_failed: true },
        });
        break;
      default:
        // TODO: Handle this `new_tasks` case
        break;
    }
  };

  return (
    <Dialog.Root onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">Clear DagRun - {dagRunId} </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body width="full">
          <Flex justifyContent="center">
            <SegmentedControl
              mb={3}
              onValueChange={onChange}
              options={[
                { label: "Clear existing tasks", value: "existing_tasks" },
                { label: "Clear only failed tasks", value: "only_failed" },
                {
                  disabled: true,
                  label: "Queue up new tasks",
                  value: "new_tasks",
                },
              ]}
              value={onlyFailed ? "only_failed" : "existing_tasks"}
            />
          </Flex>
          <ClearRunTasksAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
          <Flex justifyContent="end" mt={3}>
            <Button
              colorPalette="blue"
              loading={isPending || isPendingPatchDagRun}
              onClick={() => {
                mutate({
                  dagId,
                  dagRunId,
                  requestBody: { dry_run: false, only_failed: onlyFailed },
                });
                mutatePatchDagRun({
                  dagId,
                  dagRunId,
                  requestBody: { note },
                });
              }}
            >
              <FiRefreshCw /> Confirm
            </Button>
          </Flex>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default ClearRunDialog;
