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
import { VStack, Icon, Text, Spinner } from "@chakra-ui/react";
import { useEffect, useState, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { GoAlertFill } from "react-icons/go";

import { Button, Dialog } from "src/components/ui";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";
import { getRelativeTime } from "src/utils/datetimeUtils";

type Props = {
  readonly dagDetails?: {
    dagId: string;
    dagRunId: string;
    downstream?: boolean;
    future?: boolean;
    mapIndex?: number;
    onlyFailed?: boolean;
    past?: boolean;
    taskId: string;
    upstream?: boolean;
  };
  readonly onClose: () => void;
  readonly onConfirm?: () => void;
  readonly open: boolean;
  readonly preventRunningTask: boolean;
};

const ClearTaskInstanceConfirmationDialog = ({
  dagDetails,
  onClose,
  onConfirm,
  open,
  preventRunningTask,
}: Props) => {
  const { t: translate } = useTranslation();
  const { data, isFetching } = useClearTaskInstancesDryRun({
    dagId: dagDetails?.dagId ?? "",
    options: {
      enabled: open && Boolean(dagDetails),
      gcTime: 0,
      refetchOnMount: "always",
      refetchOnWindowFocus: false,
      staleTime: 0,
    },
    requestBody: {
      dag_run_id: dagDetails?.dagRunId ?? "",
      include_downstream: dagDetails?.downstream,
      include_future: dagDetails?.future,
      include_past: dagDetails?.past,
      include_upstream: dagDetails?.upstream,
      only_failed: dagDetails?.onlyFailed,
      task_ids: [[dagDetails?.taskId ?? "", dagDetails?.mapIndex ?? 0]],
    },
  });

  const [isReady, setIsReady] = useState(false);

  const handleConfirm = useCallback(() => {
    if (onConfirm) {
      onConfirm();
    }
    onClose();
  }, [onConfirm, onClose]);

  const taskInstances = data?.task_instances ?? [];
  const [firstInstance] = taskInstances;
  const taskCurrentState = firstInstance?.state;

  useEffect(() => {
    if (!isFetching && open && data) {
      const isInTriggeringState = taskCurrentState === "queued" || taskCurrentState === "scheduled";

      if (!preventRunningTask || !isInTriggeringState) {
        handleConfirm();
      } else {
        setIsReady(true);
      }
    }
  }, [isFetching, data, open, handleConfirm, taskCurrentState, preventRunningTask]);

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open}>
      <Dialog.Content backdrop>
        {isFetching ? (
          <VStack align="center" gap={3} justify="center" py={8}>
            <Spinner size="lg" />
            <Text color="fg.solid" fontSize="md">
              {translate("common:task.documentation")}
            </Text>
          </VStack>
        ) : isReady ? (
          <>
            <Dialog.Header>
              <VStack align="start" gap={4}>
                <Dialog.Title>
                  <Icon color="tomato" pr="2" size="lg">
                    <GoAlertFill />
                  </Icon>
                  {translate("dags:runAndTaskActions.confirmationDialog.title")}
                </Dialog.Title>
                <Dialog.Description>
                  {taskInstances.length > 0 && (
                    <>
                      {translate("dags:runAndTaskActions.confirmationDialog.description", {
                        state: taskCurrentState,
                        time:
                          firstInstance?.start_date !== null && firstInstance?.start_date !== undefined
                            ? getRelativeTime(firstInstance.start_date)
                            : undefined,
                        user:
                          (firstInstance?.unixname?.trim().length ?? 0) > 0
                            ? firstInstance?.unixname
                            : "unknown user",
                      })}
                    </>
                  )}
                </Dialog.Description>
              </VStack>
            </Dialog.Header>
            <Dialog.Footer>
              <Button colorPalette="blue" onClick={onClose}>
                {translate("common:modal.confirm")}
              </Button>
            </Dialog.Footer>
          </>
        ) : null}
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default ClearTaskInstanceConfirmationDialog;
