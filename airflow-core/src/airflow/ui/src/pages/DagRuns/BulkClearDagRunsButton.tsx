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
import { Box, Button, Flex, Heading, Text, useDisclosure, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Checkbox, Dialog } from "src/components/ui";
import SegmentedControl from "src/components/ui/SegmentedControl";
import { useBulkDagRuns } from "src/queries/useBulkDagRuns";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedDagRuns: Array<DAGRunResponse>;
};

const BulkClearDagRunsButton = ({ clearSelections, selectedDagRuns }: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["existingTasks"]);
  const [note, setNote] = useState<string | null>(null);
  const [runOnLatestVersion, setRunOnLatestVersion] = useState(false);
  const { bulkClear, error, isPending } = useBulkDagRuns({
    clearSelections,
    onSuccessConfirm: onClose,
  });

  const onlyFailed = selectedOptions.includes("onlyFailed");
  const onlyNew = selectedOptions.includes("newTasks");

  const handleClose = () => {
    setNote(null);
    onClose();
  };

  return (
    <>
      <Button colorPalette="brand" onClick={onOpen} size="sm" variant="outline">
        <CgRedo />
        {translate("dags:runAndTaskActions.clear.button", { type: translate("dagRun_other") })}
      </Button>

      <Dialog.Root onOpenChange={handleClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("dags:runAndTaskActions.clear.title", {
                  type: translate("dagRun_other"),
                })}
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

            <Text color="fg.subtle" fontSize="sm" mb={2}>
              {selectedDagRuns.length} {translate("dagRun_other")}
            </Text>
            <Box maxH="320px" overflowY="auto">
              <VStack align="stretch" gap={1}>
                {selectedDagRuns.map((dagRun) => (
                  <Text fontSize="sm" key={`${dagRun.dag_id}:${dagRun.dag_run_id}`}>
                    <Text as="span" color="fg.subtle">
                      {dagRun.dag_id}
                    </Text>{" "}
                    / {dagRun.dag_run_id}
                  </Text>
                ))}
              </VStack>
            </Box>

            <ActionAccordion note={note} setNote={setNote} />
            <ErrorAlert error={error} />
            <Flex alignItems="center" justifyContent="space-between" mt={3}>
              <Checkbox
                checked={runOnLatestVersion}
                disabled={onlyNew}
                onCheckedChange={(event) => setRunOnLatestVersion(Boolean(event.checked))}
              >
                {translate("dags:runAndTaskActions.options.runOnLatestVersion")}
              </Checkbox>
              <Button
                colorPalette="brand"
                disabled={selectedDagRuns.length === 0}
                loading={isPending}
                onClick={() => {
                  void bulkClear(selectedDagRuns, {
                    note,
                    onlyFailed,
                    onlyNew,
                    runOnLatestVersion,
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

export default BulkClearDagRunsButton;
