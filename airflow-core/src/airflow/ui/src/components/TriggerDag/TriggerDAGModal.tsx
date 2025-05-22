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
import { Heading, VStack, HStack, Spinner, Center, Text } from "@chakra-ui/react";
import React, { useState } from "react";

import { useDagServiceGetDag } from "openapi/queries";
import { Dialog, Tooltip } from "src/components/ui";
import { RadioCardItem, RadioCardRoot } from "src/components/ui/RadioCard";

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
};

const TriggerDAGModal: React.FC<TriggerDAGModalProps> = ({
  dagDisplayName,
  dagId,
  isPaused,
  onClose,
  open,
}) => {
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

  const hasSchedule = dag?.timetable_summary !== null;

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl" unmountOnExit>
      <Dialog.Content backdrop>
        <Dialog.Header paddingBottom={0}>
          <VStack align="start" gap={2} width="100%">
            <Heading size="xl">
              {runMode === RunMode.SINGLE ? "Trigger DAG" : "Run Backfill"} - {dagDisplayName}
            </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body>
          {isLoading ? (
            <Center py={6}>
              <VStack>
                <Spinner size="lg" />
                <Text mt={2}>Loading DAG information...</Text>
              </VStack>
            </Center>
          ) : isError ? (
            <Center py={6}>
              <Text color="red.500">Failed to load DAG information. Please try again.</Text>
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
                      description="Trigger a single run of this DAG"
                      label="Single Run"
                      value={RunMode.SINGLE}
                    />
                    <Tooltip content="Backfill requires a schedule" disabled={hasSchedule}>
                      <RadioCardItem
                        description="Run this DAG for a range of dates"
                        disabled={!hasSchedule}
                        label="Backfill"
                        value={RunMode.BACKFILL}
                      />
                    </Tooltip>
                  </HStack>
                </RadioCardRoot>
              ) : undefined}

              {runMode === RunMode.SINGLE ? (
                <TriggerDAGForm dagId={dagId} isPaused={isPaused} onClose={onClose} open={open} />
              ) : (
                hasSchedule && dag && <RunBackfillForm dag={dag} onClose={onClose} />
              )}
            </>
          )}
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default TriggerDAGModal;
