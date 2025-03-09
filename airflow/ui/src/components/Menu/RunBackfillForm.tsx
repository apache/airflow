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
import { Input, Box, Spacer, HStack, Field, VStack, Flex, Text } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useForm, Controller, useWatch } from "react-hook-form";

import type { DAGResponse, DAGWithLatestDagRunsResponse, BackfillPostBody } from "openapi/requests/types.gen";
import { Alert, Button } from "src/components/ui";
import { reprocessBehaviors } from "src/constants/reprocessBehaviourParams";
import { useCreateBackfill } from "src/queries/useCreateBackfill";
import { useCreateBackfillDryRun } from "src/queries/useCreateBackfillDryRun";

import { ErrorAlert } from "../ErrorAlert";
import { Checkbox } from "../ui/Checkbox";
import { RadioCardItem, RadioCardLabel, RadioCardRoot } from "../ui/RadioCard";

type RunBackfillFormProps = {
  readonly dag: DAGResponse | DAGWithLatestDagRunsResponse;
  readonly onClose: () => void;
};
const today = new Date().toISOString().slice(0, 16);

const RunBackfillForm = ({ dag, onClose }: RunBackfillFormProps) => {
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});

  const { control, handleSubmit, reset, watch } = useForm<BackfillPostBody>({
    defaultValues: {
      dag_id: dag.dag_id,
      dag_run_conf: {},
      from_date: undefined,
      max_active_runs: undefined,
      reprocess_behavior: undefined,
      run_backwards: undefined,
      to_date: undefined,
    },
    mode: "onBlur",
  });
  const values = useWatch<BackfillPostBody>({
    control,
  });

  const { data, isPending: isPendingDryRun } = useCreateBackfillDryRun({
    requestBody: {
      requestBody: {
        dag_id: dag.dag_id,
        dag_run_conf: undefined,
        from_date: values.from_date ?? today,
        max_active_runs: values.max_active_runs ?? 0,
        reprocess_behavior: values.reprocess_behavior,
        run_backwards: values.run_backwards ?? false,
        to_date: values.to_date ?? today,
      },
    },
  });

  const { createBackfill, dateValidationError, error, isPending } = useCreateBackfill({
    onSuccessConfirm: onClose,
  });

  useEffect(() => {
    if (Boolean(dateValidationError)) {
      setErrors((prev) => ({ ...prev, date: dateValidationError }));
    }
  }, [dateValidationError]);

  const dataIntervalStart = watch("from_date");
  const dataIntervalEnd = watch("to_date");

  const onSubmit = (fdata: BackfillPostBody) => {
    createBackfill({
      requestBody: fdata,
    });
  };

  const onCancel = (fdata: BackfillPostBody) => {
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

  return (
    <>
      <VStack alignItems="stretch" gap={2} p={10}>
        <Box>
          <Text fontSize="md" fontWeight="medium" mb={1}>
            Date Range
          </Text>
          <HStack w="full">
            <Controller
              control={control}
              name="from_date"
              render={({ field }) => (
                <Field.Root invalid={Boolean(errors.date)}>
                  <Input
                    {...field}
                    max={dataIntervalEnd || today}
                    onBlur={resetDateError}
                    size="sm"
                    type="datetime-local"
                  />
                </Field.Root>
              )}
            />
            <Controller
              control={control}
              name="to_date"
              render={({ field }) => (
                <Field.Root invalid={Boolean(errors.date)}>
                  <Input
                    {...field}
                    max={today}
                    min={dataIntervalStart || undefined}
                    onBlur={resetDateError}
                    size="sm"
                    type="datetime-local"
                  />
                </Field.Root>
              )}
            />
          </HStack>
        </Box>
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
              <RadioCardLabel fontSize="md">Reprocess Behaviour</RadioCardLabel>
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
          name="run_backwards"
          render={({ field }) => (
            <Checkbox checked={field.value} colorPalette="blue" onChange={field.onChange}>
              Run Backwards
            </Checkbox>
          )}
        />
        <Spacer />
        <Controller
          control={control}
          name="max_active_runs"
          render={({ field }) => (
            <HStack>
              <Input {...field} placeholder="" type="number" width={24} />
              <Flex>Max Active Runs</Flex>
            </HStack>
          )}
        />
        <Spacer />
        {affectedTasks.total_entries > 0 ? (
          <Alert>{affectedTasks.total_entries} runs will be triggered</Alert>
        ) : undefined}
      </VStack>
      <ErrorAlert error={errors.date ?? error} />
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          <Spacer />
          <Button onClick={() => void handleSubmit(onCancel)()}>Cancel</Button>
          <Button
            colorPalette="blue"
            disabled={Boolean(errors.date) || isPendingDryRun || affectedTasks.total_entries === 0}
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
