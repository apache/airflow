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
import { Center, HStack, Spinner, Text, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useDagServiceGetDag } from "openapi/queries";
import { Modal, Tooltip } from "src/components/ui";
import { RadioCardItem, RadioCardRoot } from "src/components/ui/RadioCard";
import { useTrigger } from "src/queries/useTrigger";

import RunBackfillForm from "../DagActions/RunBackfillForm";
import TriggerDAGForm from "./TriggerDAGForm";

enum RunMode {
  BACKFILL = "backfill",
  SINGLE = "single",
}

type TriggerDAGModalProps = {
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly isPaused: boolean;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly prefillConfig?:
    | {
        conf: Record<string, unknown> | undefined;
        logicalDate: string | undefined;
        runId: string;
      }
    | undefined;
};

const TriggerDAGModal = ({
  dagDisplayName,
  dagId,
  isPaused,
  onClose,
  open,
  prefillConfig,
}: TriggerDAGModalProps) => {
  const { t: translate } = useTranslation("components");
  const [runMode, setRunMode] = useState<RunMode>(RunMode.SINGLE);
  const {
    data: dag,
    isError,
    isLoading,
  } = useDagServiceGetDag(
    {
      dagId,
    },
    undefined,
    {
      enabled: open,
    },
  );

  const isBackfillable = dag?.is_backfillable ?? false;
  const hasSchedule = dag?.timetable_summary !== null;
  const isPartitioned = dag ? dag.timetable_partitioned : false;
  const isPartitionedAtRuntime = dag?.timetable_partitioned_at_runtime ?? false;
  const suggestedPartitionKey = dag?.suggested_partition_key;
  const { error, isPending, triggerDagRun } = useTrigger({ dagId, onSuccessConfirm: onClose });
  const maxDisplayLength = 59; // hard-coded length to prevent dag name overflowing the modal
  const nameOverflowing = dagDisplayName.length > maxDisplayLength;

  return (
    <Modal
      lazyMount
      onOpenChange={onClose}
      open={open}
      title={
        <>
          {runMode === RunMode.SINGLE ? translate("triggerDag.title") : translate("backfill.title")} -{" "}
          {nameOverflowing ? <br /> : undefined} {dagDisplayName}
        </>
      }
      unmountOnExit
    >
      {isLoading ? (
        <Center py={6}>
          <VStack>
            <Spinner size="lg" />
            <Text mt={2}>{translate("triggerDag.loading")}</Text>
          </VStack>
        </Center>
      ) : isError ? (
        <Center py={6}>
          <Text color="fg.error">{translate("triggerDag.loadingFailed")}</Text>
        </Center>
      ) : (
        <>
          {dag ? (
            <RadioCardRoot
              my={4}
              onChange={(event) => {
                setRunMode((event.target as HTMLInputElement).value as RunMode);
              }}
              value={runMode}
            >
              <HStack align="stretch">
                <RadioCardItem
                  description={translate("triggerDag.selectDescription")}
                  label={translate("triggerDag.selectLabel")}
                  value={RunMode.SINGLE}
                />
                <Tooltip
                  content={translate("backfill.scheduleNotBackfillable")}
                  disabled={isBackfillable}
                  portalled
                >
                  <RadioCardItem
                    description={translate("backfill.selectDescription")}
                    disabled={!isBackfillable}
                    label={translate("backfill.selectLabel")}
                    value={RunMode.BACKFILL}
                  />
                </Tooltip>
              </HStack>
            </RadioCardRoot>
          ) : undefined}

          {runMode === RunMode.SINGLE ? (
            <TriggerDAGForm
              dagDisplayName={dagDisplayName}
              dagId={dagId}
              error={error}
              hasSchedule={hasSchedule}
              isPartitioned={isPartitioned}
              isPartitionedAtRuntime={isPartitionedAtRuntime}
              isPaused={isPaused}
              isPending={isPending}
              onSubmitTrigger={triggerDagRun}
              open={open}
              prefillConfig={prefillConfig}
              suggestedPartitionKey={suggestedPartitionKey}
            />
          ) : (
            isBackfillable && dag && <RunBackfillForm dag={dag} onClose={onClose} />
          )}
        </>
      )}
    </Modal>
  );
};

export default TriggerDAGModal;
