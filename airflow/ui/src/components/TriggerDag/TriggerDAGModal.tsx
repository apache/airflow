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
import { Heading, VStack } from "@chakra-ui/react";
import React, { useCallback, useEffect, useMemo, useState } from "react";

import { Alert, Dialog } from "src/components/ui";

import { TogglePause } from "../TogglePause";
import TriggerDAGForm from "./TriggerDAGForm";
import type { DagParams } from "./TriggerDag";
import { TriggerDag as triggerDag } from "./TriggerDag";

type TriggerDAGModalProps = {
  dagDisplayName: string;
  dagId: string;
  isPaused: boolean;
  onClose: () => void;
  open: boolean;
};

const TriggerDAGModal: React.FC<TriggerDAGModalProps> = ({
  dagDisplayName,
  dagId,
  isPaused,
  onClose,
  open,
}) => {
  const initialDagParams = useMemo(
    () => ({
      configJson: "{}",
      dagId,
      dataIntervalEnd: "",
      dataIntervalStart: "",
      runId: "",
    }),
    [dagId],
  );

  const [dagParams, setDagParams] = useState<DagParams>(initialDagParams);

  const handleTrigger = useCallback(
    (updatedDagParams: DagParams) => {
      triggerDag(updatedDagParams);
      onClose();
    },
    [onClose],
  );

  useEffect(() => {
    if (!open) {
      setDagParams(initialDagParams);
    }
  }, [open, initialDagParams]);

  return (
    <Dialog.Root onOpenChange={onClose} open={open} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              Trigger DAG - {dagDisplayName}{" "}
              <TogglePause dagId={dagParams.dagId} isPaused={isPaused} />
            </Heading>
            {isPaused ? (
              <Alert status="warning" title="Paused DAG">
                You can not trigger a paused DAG, Unpause DAG to trigger
              </Alert>
            ) : undefined}
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body>
          <TriggerDAGForm
            dagParams={dagParams}
            isPaused={isPaused}
            onClose={onClose}
            onTrigger={handleTrigger}
            setDagParams={setDagParams}
          />
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default TriggerDAGModal;
