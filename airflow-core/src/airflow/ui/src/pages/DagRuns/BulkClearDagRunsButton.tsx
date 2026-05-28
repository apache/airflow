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

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { ActionErrors } from "src/components/ActionErrors";
import { Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useBulkClearDagRuns } from "src/queries/useBulkClearDagRuns";
import { useBulkClearDagRunsDryRun } from "src/queries/useBulkClearDagRunsDryRun";

type Props = {
  readonly deselectKeys: (keys: Array<string>) => void;
  readonly selectedDagRuns: Array<DAGRunResponse>;
};

const BulkClearDagRunsButton = ({ deselectKeys, selectedDagRuns }: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const { onClose, onOpen, open } = useDisclosure();
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["existingTasks"]);
  const [note, setNote] = useState<string | null>(null);
  const { bulkClear, data, isPending } = useBulkClearDagRuns({
    deselectKeys,
    onSuccessConfirm: onClose,
  });

  const handleClose = () => {
    setNote(null);
    onClose();
  };

  const onlyFailed = selectedOptions.includes("onlyFailed");
  const onlyNew = selectedOptions.includes("newTasks");

  const { data: affectedTasks, isFetching } = useBulkClearDagRunsDryRun(open, selectedDagRuns, {
    onlyFailed,
    onlyNew,
  });

  return (
    <>
      <Button onClick={onOpen} size="sm" variant="outline">
        <CgRedo />
        {translate("dags:runAndTaskActions.clear.button", { type: translate("dagRun_other") })}
      </Button>

      <Dialog.Root onOpenChange={handleClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("dags:runAndTaskActions.clear.title", { type: translate("dagRun_other") })}
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />
          <Dialog.Body width="full">
            <Flex justifyContent="center" mb={4}>
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
                    value: "newTasks",
                  },
                ]}
              />
            </Flex>
            <ActionAccordion affectedTasks={affectedTasks} groupByRunId note={note} setNote={setNote} />
            <ActionErrors actionResponse={data?.clear} error={undefined} />
            <Flex justifyContent="end" mt={3}>
              <Button
                disabled={affectedTasks.total_entries === 0}
                loading={isPending || isFetching}
                onClick={() => {
                  void bulkClear(selectedDagRuns, { note, onlyFailed, onlyNew });
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

export default BulkClearDagRunsButton;
