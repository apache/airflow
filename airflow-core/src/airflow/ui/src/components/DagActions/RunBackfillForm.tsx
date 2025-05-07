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
import { Input, Box, Spacer, HStack, Field, VStack, Flex, Text, Skeleton } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useEffect, useState } from "react";
import { useForm, Controller, useWatch } from "react-hook-form";

import type { DAGResponse, DAGWithLatestDagRunsResponse, BackfillPostBody } from "openapi/requests/types.gen";
import { Button } from "src/components/ui";
import { reprocessBehaviors } from "src/constants/reprocessBehaviourParams";
import { useCreateBackfill } from "src/queries/useCreateBackfill";
import { useCreateBackfillDryRun } from "src/queries/useCreateBackfillDryRun";
import { useDagParams } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";
import { useTogglePause } from "src/queries/useTogglePause";
import { pluralize } from "src/utils";

import ConfigForm from "../ConfigForm";
import { DateTimeInput } from "../DateTimeInput";
import { ErrorAlert } from "../ErrorAlert";
import type { DagRunTriggerParams } from "../TriggerDag/TriggerDAGForm";
import { Checkbox } from "../ui/Checkbox";
import { RadioCardItem, RadioCardLabel, RadioCardRoot } from "../ui/RadioCard";

type RunBackfillFormProps = {
  readonly dag: DAGResponse | DAGWithLatestDagRunsResponse;
  readonly onClose: () => void;
};
const today = new Date().toISOString().slice(0, 16);

type BackfillFormProps = DagRunTriggerParams & Omit<BackfillPostBody, "dag_run_conf">;

const RunBackfillForm = ({ dag, onClose }: RunBackfillFormProps) => {
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});
  const [unpause, setUnpause] = useState(true);
  const [formError, setFormError] = useState(false);
  const initialParamsDict = useDagParams(dag.dag_id, true);
  const { conf } = useParamStore();
  const { control, handleSubmit, reset, watch } = useForm<BackfillFormProps>({
    defaultValues: {
      conf,
      dag_id: dag.dag_id,
      from_date: "",
      max_active_runs: 1,
      reprocess_behavior: "none",
      run_backwards: false,
      to_date: "",
    },
    mode: "onBlur",
  });
  const values = useWatch<BackfillFormProps>({
    control,
  });

  const { data, isPending: isPendingDryRun } = useCreateBackfillDryRun({
    requestBody: {
      requestBody: {
        dag_id: dag.dag_id,
        dag_run_conf: undefined,
        from_date: values.from_date ?? "",
        max_active_runs: values.max_active_runs ?? 1,
        reprocess_behavior: values.reprocess_behavior,
        run_backwards: values.run_backwards ?? false,
        to_date: values.to_date ?? "",
      },
    },
  });

  const { mutate: togglePause } = useTogglePause({ dagId: dag.dag_id });

  const { createBackfill, dateValidationError, error, isPending } = useCreateBackfill({
    onSuccessConfirm: onClose,
  });

  useEffect(() => {
    if (Boolean(dateValidationError)) {
      setErrors((prev) => ({ ...prev, date: dateValidationError }));
    }
  }, [dateValidationError]);

  useEffect(() => {
    if (conf) {
      reset((prevValues) => ({
        ...prevValues,
        conf,
      }));
    }
  }, [conf, reset]);

  const dataIntervalStart = watch("from_date");
  const dataIntervalEnd = watch("to_date");
  const noDataInterval = !Boolean(dataIntervalStart) || !Boolean(dataIntervalEnd);
  const dataIntervalInvalid = dayjs(dataIntervalStart).isAfter(dayjs(dataIntervalEnd));

  const onSubmit = (fdata: BackfillFormProps) => {
    if (unpause && dag.is_paused) {
      togglePause({
        dagId: dag.dag_id,
        requestBody: {
          is_paused: false,
        },
      });
    }
    createBackfill({
      requestBody: {
        ...fdata,
        dag_run_conf: JSON.parse(fdata.conf) as Record<string, unknown>,
      },
    });
  };

  const onCancel = (fdata: BackfillFormProps) => {
    reset(fdata);
    onClose();
  };

  const resetDateError = () => {
    setErrors((prev) => ({ ...prev, date: undefined }));
  };

  const affectedTasks = data ?? {
    backfills: [],
    total_entries: 0,
  };

  const inlineMessage = isPendingDryRun ? (
    <Skeleton height="20px" width="100px" />
  ) : affectedTasks.total_entries > 0 ? (
    <Text color="fg.success" fontSize="sm">
      {pluralize("run", affectedTasks.total_entries)} will be triggered
    </Text>
  ) : (
    <Text color="fg.error" fontSize="sm" fontWeight="medium">
      No runs matching selected criteria.
    </Text>
  );

  return (
    <>
      <ErrorAlert error={errors.date ?? error} />
      <VStack alignItems="stretch" gap={2} pt={4}>
        <Box>
          <Text fontSize="md" fontWeight="semibold" mb={3}>
            Date Range
          </Text>
          <HStack alignItems="flex-start" w="full">
            <Controller
              control={control}
              name="from_date"
              render={({ field }) => (
                <Field.Root invalid={Boolean(errors.date) || dataIntervalInvalid} required>
                  <Field.Label>From</Field.Label>
                  <DateTimeInput {...field} max={today} onBlur={resetDateError} size="sm" />
                  <Field.ErrorText>Start Date must be before the End Date</Field.ErrorText>
                </Field.Root>
              )}
            />
            <Controller
              control={control}
              name="to_date"
              render={({ field }) => (
                <Field.Root invalid={Boolean(errors.date) || dataIntervalInvalid} required>
                  <Field.Label>To</Field.Label>
                  <DateTimeInput {...field} max={today} onBlur={resetDateError} size="sm" />
                </Field.Root>
              )}
            />
          </HStack>
        </Box>
        {noDataInterval || dataIntervalInvalid ? undefined : <Box>{inlineMessage}</Box>}
        <Spacer />
        <Controller
          control={control}
          name="reprocess_behavior"
          render={({ field }) => (
            <RadioCardRoot
              defaultValue={field.value}
              onChange={(event) => {
                field.onChange(event);
              }}
            >
              <RadioCardLabel fontSize="md" fontWeight="semibold" mb={3}>
                Reprocess Behaviour
              </RadioCardLabel>
              <HStack>
                {reprocessBehaviors.map((item) => (
                  <RadioCardItem
                    colorPalette="blue"
                    indicatorPlacement="start"
                    key={item.value}
                    label={item.label}
                    value={item.value}
                  />
                ))}
              </HStack>
            </RadioCardRoot>
          )}
        />
        <Spacer />
        <Controller
          control={control}
          name="max_active_runs"
          render={({ field }) => (
            <HStack>
              <Input
                {...field}
                max={dag.max_active_runs ?? undefined}
                min={1}
                placeholder=""
                type="number"
                width={24}
              />
              <Flex>Max Active Runs</Flex>
            </HStack>
          )}
        />
        <Spacer />
        <Controller
          control={control}
          name="run_backwards"
          render={({ field }) => (
            <Checkbox checked={field.value} colorPalette="blue" onChange={field.onChange}>
              Run Backwards
            </Checkbox>
          )}
        />
        <Spacer />
        {dag.is_paused ? (
          <>
            <Checkbox checked={unpause} colorPalette="blue" onChange={() => setUnpause(!unpause)}>
              Unpause {dag.dag_display_name} on trigger
            </Checkbox>
            <Spacer />
          </>
        ) : undefined}

        <ConfigForm
          control={control}
          errors={errors}
          initialParamsDict={initialParamsDict}
          setErrors={setErrors}
          setFormError={setFormError}
        />
      </VStack>
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          <Spacer />
          <Button onClick={() => void handleSubmit(onCancel)()}>Cancel</Button>
          <Button
            colorPalette="blue"
            disabled={
              Boolean(errors.date) || isPendingDryRun || formError || affectedTasks.total_entries === 0
            }
            loading={isPending}
            onClick={() => void handleSubmit(onSubmit)()}
          >
            Run Backfill
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default RunBackfillForm;
