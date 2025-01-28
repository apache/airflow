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

import type { DAGRunPatchStates, DAGRunResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { Button, Dialog } from "src/components/ui";
import { usePatchDagRun } from "src/queries/usePatchDagRun";

import MarkAsAccordion from "../MarkAsAccordion";

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly state: DAGRunPatchStates;
};

const MarkRunAsDialog = ({ dagRun, onClose, open, state }: Props) => {
  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;

  const [note, setNote] = useState<string | null>(dagRun.note);
  const { isPending, mutate } = usePatchDagRun({ dagId, dagRunId, onSuccess: onClose });

  return (
    <Dialog.Root onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              <strong>Mark DagRun as {state}:</strong> {dagRunId} <StateBadge state={state} />
            </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body width="full">
          <MarkAsAccordion note={note} setNote={setNote} />
          <Flex justifyContent="end" mt={3}>
            <Button
              colorPalette="blue"
              loading={isPending}
              onClick={() => {
                mutate({
                  dagId,
                  dagRunId,
                  requestBody: { note, state },
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

export default MarkRunAsDialog;
