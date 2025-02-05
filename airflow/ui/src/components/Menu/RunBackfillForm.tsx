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
import { Input, Button, Box, Spacer, HStack, Field, VStack } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useForm, Controller } from "react-hook-form";

import type { DAGResponse, DAGWithLatestDagRunsResponse, BackfillPostBody } from "openapi/requests/types.gen";
import { useBackfillServiceCreateBackfillDryRun } from "src/queries/useBackfillServiceCreateBackfillDryRun";

import { ErrorAlert } from "../ErrorAlert";
import { RadioCardItem, RadioCardLabel, RadioCardRoot } from "../ui/RadioCard";

type RunBackfillFormProps = {
  readonly dag: DAGResponse | DAGWithLatestDagRunsResponse;
  readonly onClose: () => void;
};

// export type BackfillPostBody = {
//   conf: string;
//   dagId: string;
//   dataIntervalEnd: string;
//   dataIntervalStart: string;
//   reprocessBehavior: string;
// };

const RunBackfillForm = ({ dag, onClose }: RunBackfillFormProps) => {
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});
  const { dateValidationError, error: errorTrigger } = useBackfillServiceCreateBackfillDryRun({
    dagId: dag.dag_id,
    onSuccessConfirm: onClose,
  });

  const { control, handleSubmit, reset, watch } = useForm<BackfillPostBody>({
    defaultValues: {
      dag_id: "",
      dag_run_conf: {},
      from_date: "",
      max_active_runs: 0,
      reprocess_behavior: "none",
      run_backwards: false,
      to_date: "",
    },
  });

  useEffect(() => {
    if (Boolean(dateValidationError)) {
      setErrors((prev) => ({ ...prev, date: dateValidationError }));
    }
  }, [dateValidationError]);

  const dataIntervalStart = watch("from_date");
  const dataIntervalEnd = watch("to_date");

  const onSubmit = (data: BackfillPostBody) => {
    reset(data);
    onClose();
  };

  const onCancel = (data: BackfillPostBody) => {
    reset(data);
    onClose();
  };

  const resetDateError = () => {
    setErrors((prev) => ({ ...prev, date: undefined }));
  };

  const reprocessBehaviors = [
    { label: "Missing Runs", value: "failed" },
    { label: "Missing and Errored Runs", value: "completed" },
    { label: "All Runs", value: "none" },
  ];

  return (
    <>
      <VStack align="baseline" display="flex" p={10}>
        <HStack w="full">
          <Controller
            control={control}
            name="from_date"
            render={({ field }) => (
              <Field.Root invalid={Boolean(errors.date)}>
                <Field.Label fontSize="md">Data Interval Start Date</Field.Label>
                <Input
                  {...field}
                  max={dataIntervalEnd || undefined}
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
                <Field.Label fontSize="md">Data Interval End Date</Field.Label>
                <Input
                  {...field}
                  min={dataIntervalStart || undefined}
                  onBlur={resetDateError}
                  size="sm"
                  type="datetime-local"
                />
              </Field.Root>
            )}
          />
        </HStack>
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
              <RadioCardLabel fontSize="md">RUN TYPES</RadioCardLabel>
              <VStack align="stretch">
                {reprocessBehaviors.map((item) => (
                  <RadioCardItem
                    colorPalette="blue"
                    indicatorPlacement="start"
                    key={item.value}
                    label={item.label}
                    value={item.value}
                  />
                ))}
              </VStack>
            </RadioCardRoot>
          )}
        />
      </VStack>
      <ErrorAlert error={errors.date ?? errorTrigger} />
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          <Spacer />
          <Button
            disabled={Boolean(errors.conf) || Boolean(errors.date)}
            onClick={() => void handleSubmit(onCancel)()}
          >
            Cancel
          </Button>
          <Button
            colorPalette="blue"
            disabled={Boolean(errors.conf) || Boolean(errors.date)}
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
