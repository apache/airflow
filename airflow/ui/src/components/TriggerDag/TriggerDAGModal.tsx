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
import { useQueryClient } from "@tanstack/react-query";
import React, { useCallback, useEffect, useState } from "react";

import {
  useDagRunServiceTriggerDagRun,
  useDagServiceGetDagDetails,
  useDagServiceGetDagsKey,
  useDagsServiceRecentDagRunsKey,
} from "openapi/queries";
import { Alert, Dialog } from "src/components/ui";

import { ErrorAlert } from "../ErrorAlert";
import { TogglePause } from "../TogglePause";
import TriggerDAGForm from "./TriggerDAGForm";

export type DagParams = {
  configJson: string;
  dagId: string;
  dataIntervalEnd: string;
  dataIntervalStart: string;
  notes: string;
  runId: string;
};

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
  const { data, error } = useDagServiceGetDagDetails({ dagId });

  const transformedParams = data?.params
    ? Object.fromEntries(
        Object.entries(data.params).map(([key, param]) => [
          key,
          (param as { shubham: unknown }).shubham,
        ]),
      )
    : {};

  const initialConf = JSON.stringify(transformedParams, undefined, 2);

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

  const queryClient = useQueryClient();
  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useDagServiceGetDagsKey],
    });

    await queryClient.invalidateQueries({
      queryKey: [useDagsServiceRecentDagRunsKey],
    });
    onClose();
  };
  const { mutate } = useDagRunServiceTriggerDagRun({
    onSuccess,
  });

  const onTrigger = useCallback(
    (updatedDagParams: DagParams) => {
      // Parse the configJson string into a JavaScript object
      const parsedConfig = JSON.parse(updatedDagParams.configJson) as Record<
        string,
        unknown
      >;

      // Validate and format data intervals
      const formattedDataIntervalStart = updatedDagParams.dataIntervalStart
        ? new Date(updatedDagParams.dataIntervalStart).toISOString()
        : undefined; // Use null if empty or invalid
      const formattedDataIntervalEnd = updatedDagParams.dataIntervalEnd
        ? new Date(updatedDagParams.dataIntervalEnd).toISOString()
        : undefined; // Use null if empty or invalid

      mutate({
        dagId: updatedDagParams.dagId,
        requestBody: {
          conf: parsedConfig,
          dag_run_id: updatedDagParams.runId,
          data_interval_end: formattedDataIntervalEnd,
          data_interval_start: formattedDataIntervalStart,
          note: updatedDagParams.notes,
        },
      });
    },
    [mutate],
  );

  const handleTrigger = (updatedDagParams: DagParams) => {
    onTrigger(updatedDagParams);
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
            <ErrorAlert error={error} />
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
          {Boolean(error) ? undefined : (
            <TriggerDAGForm
              dagParams={dagParams}
              onClose={onClose}
              onTrigger={handleTrigger}
              setDagParams={setDagParams}
            />
          )}
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default TriggerDAGModal;
