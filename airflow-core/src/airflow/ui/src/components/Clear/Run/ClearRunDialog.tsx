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
import { CloseButton, Dialog, Flex, Heading, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import { useDagServiceGetDagDetails } from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { Button, Checkbox } from "src/components/ui";
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
  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;
  const { t: translate } = useTranslation();

  const [note, setNote] = useState<string | null>(dagRun.note);
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(["existingTasks"]);
  const onlyFailed = selectedOptions.includes("onlyFailed");
  const [runOnLatestVersion, setRunOnLatestVersion] = useState(false);

  // Get current DAG's bundle version to compare with DAG run's bundle version
  const { data: dagDetails } = useDagServiceGetDagDetails({
    dagId,
  });

  const { data: affectedTasks = { task_instances: [], total_entries: 0 } } = useClearDagRunDryRun({
    dagId,
    dagRunId,
    requestBody: { only_failed: onlyFailed, run_on_latest_version: runOnLatestVersion },
  });

  const { isPending, mutate } = useClearDagRun({
    dagId,
    dagRunId,
    onSuccessConfirm: onClose,
  });

  const { isPending: isPendingPatchDagRun, mutate: mutatePatchDagRun } = usePatchDagRun({
    dagId,
    dagRunId,
    onSuccess: onClose,
  });

  // Check if bundle versions are different
  const currentDagBundleVersion = dagDetails?.bundle_version;
  const dagRunBundleVersion = dagRun.bundle_version;
  const bundleVersionsDiffer = currentDagBundleVersion !== dagRunBundleVersion;
  const shouldShowBundleVersionOption =
    bundleVersionsDiffer && dagRunBundleVersion !== null && dagRunBundleVersion !== "";

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl">
      <Dialog.Backdrop />
      <Dialog.Positioner>
        <Dialog.Content>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                <strong>
                  {translate("dags:runAndTaskActions.clear.title", { type: translate("dagRun_one") })}:{" "}
                </strong>{" "}
                {dagRunId}
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
            <CloseButton size="sm" />
          </Dialog.CloseTrigger>

          <Dialog.Body width="full">
            <Flex justifyContent="center">
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
                    disabled: true,
                    label: translate("dags:runAndTaskActions.options.queueNew"),
                    value: "new_tasks",
                  },
                ]}
              />
            </Flex>
            <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
            <Flex
              {...(shouldShowBundleVersionOption ? { alignItems: "center" } : {})}
              justifyContent={shouldShowBundleVersionOption ? "space-between" : "end"}
              mt={3}
            >
              {shouldShowBundleVersionOption ? (
                <Checkbox
                  checked={runOnLatestVersion}
                  onCheckedChange={(event) => setRunOnLatestVersion(Boolean(event.checked))}
                >
                  {translate("dags:runAndTaskActions.options.runOnLatestVersion")}
                </Checkbox>
              ) : undefined}
              <Button
                colorPalette="brand"
                disabled={affectedTasks.total_entries === 0}
                loading={isPending || isPendingPatchDagRun}
                onClick={() => {
                  mutate({
                    dagId,
                    dagRunId,
                    requestBody: {
                      dry_run: false,
                      only_failed: onlyFailed,
                      run_on_latest_version: runOnLatestVersion,
                    },
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
                <CgRedo /> {translate("modal.confirm")}
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Positioner>
    </Dialog.Root>
  );
};

export default ClearRunDialog;
