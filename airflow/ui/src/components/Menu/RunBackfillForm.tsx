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
import { Input, Button, Box, Spacer, HStack, Field, RadioGroup, VStack } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useForm, Controller } from "react-hook-form";

import type { DAGResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { useTrigger } from "src/queries/useTrigger";

import { ErrorAlert } from "../ErrorAlert";

type RunBackfillFormProps = {
  readonly dag: DAGResponse | DAGWithLatestDagRunsResponse;
  readonly onClose: () => void;
};

export type DagRunTriggerParams = {
  conf: string;
  dagId: string;
  dataIntervalEnd: string;
  dataIntervalStart: string;
  runType: string;
};

const RunBackfillForm = ({ dag, onClose }: RunBackfillFormProps) => {
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});
  const {
    dateValidationError,
    error: errorTrigger,
    isPending,
  } = useTrigger({ dagId: dag.dag_id, onSuccessConfirm: onClose });

  const { control, handleSubmit, reset, watch } = useForm<DagRunTriggerParams>({
    defaultValues: {
      dagId: "",
      dataIntervalEnd: "",
      dataIntervalStart: "",
      runType: "",
    },
  });

  useEffect(() => {
    if (Boolean(dateValidationError)) {
      setErrors((prev) => ({ ...prev, date: dateValidationError }));
    }
  }, [dateValidationError]);

  const dataIntervalStart = watch("dataIntervalStart");
  const dataIntervalEnd = watch("dataIntervalEnd");
  const runType = watch("runType");

  const onSubmit = (data: DagRunTriggerParams) => {
    reset(data);
  };

  const onCancel = (data: DagRunTriggerParams) => {
    reset(data);
  };

  const resetDateError = () => {
    setErrors((prev) => ({ ...prev, date: undefined }));
  };

  const runTypes = [
    { label: "Missing Runs", value: "1" },
    { label: "Missing and Errored Runs", value: "2" },
    { label: "All Runs", value: "3" },
  ];

  return (
    <>
      <VStack align="baseline" display="flex" p={10}>
        <HStack w="full">
          <Controller
            control={control}
            name="dataIntervalStart"
            render={({ field }) => (
              <Field.Root invalid={Boolean(errors.date)}>
                <Field.Label fontSize="md">Data Interval Start Date</Field.Label>
                <Input
                  {...field}
                  max={dataIntervalEnd || undefined}
                  onBlur={resetDateError}
                  placeholder="yyyy-mm-ddThh:mm"
                  size="sm"
                  type="datetime-local"
                />
              </Field.Root>
            )}
          />

          <Controller
            control={control}
            name="dataIntervalEnd"
            render={({ field }) => (
              <Field.Root invalid={Boolean(errors.date)}>
                <Field.Label fontSize="md">Data Interval End Date</Field.Label>
                <Input
                  {...field}
                  min={dataIntervalStart || undefined}
                  onBlur={resetDateError}
                  placeholder="yyyy-mm-ddThh:mm"
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
          name="runType"
          render={({ field }) => (
            <RadioGroup.Root
              {...field}
              onBlur={field.onBlur}
              onChange={field.onChange}
              onValueChange={(value) => field.onChange(value)}
              value={runType}
            >
              <RadioGroup.Label>RUN TYPES</RadioGroup.Label>
              <VStack align="baseline">
                {runTypes.map((item) => (
                  <RadioGroup.Item key={item.value} value={item.value}>
                    <RadioGroup.ItemControl />
                    <RadioGroup.ItemText>{item.label}</RadioGroup.ItemText>
                  </RadioGroup.Item>
                ))}
              </VStack>
            </RadioGroup.Root>
          )}
        />
      </VStack>
      <ErrorAlert error={errors.date ?? errorTrigger} />
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          <Spacer />
          <Button
            disabled={Boolean(errors.conf) || Boolean(errors.date) || isPending}
            onClick={() => void handleSubmit(onCancel)()}
          >
            Cancel
          </Button>
          <Button
            colorPalette="blue"
            disabled={Boolean(errors.conf) || Boolean(errors.date) || isPending}
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
