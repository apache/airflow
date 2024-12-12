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
import React, { useEffect, useState } from "react";

import { useDagServiceGetDagDetails } from "openapi/queries";
import { Alert, Dialog } from "src/components/ui";

import { ErrorAlert } from "../ErrorAlert";
import { TogglePause } from "../TogglePause";
import TriggerDAGForm from "./TriggerDAGForm";
import type { DagParams } from "./useTriggerDag";
import { useTriggerDag } from "./useTriggerDag";

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
  const { error: errorTrigger, triggerDag } = useTriggerDag();
  const { data, error } = useDagServiceGetDagDetails({ dagId });
  let initialConf = "{}";
  let parseError: unknown = undefined;

  if (!Boolean(error)) {
    try {
      const transformedParams = data?.params
        ? Object.fromEntries(
            Object.entries(data.params).map(([key, param]) => [
              key,
              (param as { value: unknown }).value,
            ]),
          )
        : {};

      initialConf = JSON.stringify(transformedParams, undefined, 2);
    } catch (_error) {
      parseError = _error;
    }
  }

  const [dagParams, setDagParams] = useState<DagParams>({
    configJson: initialConf,
    dagId,
    dataIntervalEnd: "",
    dataIntervalStart: "",
    notes: "",
    runId: "",
  });

  useEffect(() => {
    const newConfigJson = Boolean(error) ? "{}" : initialConf; // Fallback to "{}" if there's an error

    setDagParams((prevParams) => ({
      ...prevParams,
      configJson: newConfigJson,
    }));
  }, [initialConf, error]);

  const handleTrigger = (updatedDagParams: DagParams) => {
    triggerDag(updatedDagParams);
    onClose();
  };

  useEffect(() => {
    if (!open) {
      setDagParams({
        configJson: initialConf,
        dagId,
        dataIntervalEnd: "",
        dataIntervalStart: "",
        notes: "",
        runId: "",
      });
    }
  }, [open, initialConf, dagId]);

  return (
    <Dialog.Root onOpenChange={onClose} open={open} size="xl">
      <ErrorAlert error={errorTrigger} />
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              Trigger DAG - {dagDisplayName}{" "}
              <TogglePause
                dagId={dagParams.dagId}
                isPaused={isPaused}
                skipConfirm
              />
            </Heading>
            <ErrorAlert error={error ?? parseError} />
            {isPaused ? (
              <Alert status="warning" title="Paused DAG">
                Triggering will create a DAG run, but it will not start until
                the DAG is unpaused.
              </Alert>
            ) : undefined}
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body>
          <TriggerDAGForm
            dagParams={dagParams}
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
