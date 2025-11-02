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
import dayjs from "dayjs";
import { useEffect, useState } from "react";
import { useForm, Controller, useWatch } from "react-hook-form";
import { useTranslation } from "react-i18next";

import type { DAGResponse, DAGWithLatestDagRunsResponse, BackfillPostBody } from "openapi/requests/types.gen";
import { Button } from "src/components/ui";
import { reprocessBehaviors } from "src/constants/reprocessBehaviourParams";
import { useCreateBackfill } from "src/queries/useCreateBackfill";
import { useCreateBackfillDryRun } from "src/queries/useCreateBackfillDryRun";
import { useDagParams } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";
import { useTogglePause } from "src/queries/useTogglePause";

import ConfigForm from "../ConfigForm";
import { DateTimeInput } from "../DateTimeInput";
import { ErrorAlert } from "../ErrorAlert";
import type { DagRunTriggerParams } from "../TriggerDag/TriggerDAGForm";
import { Checkbox } from "../ui/Checkbox";
import { RadioCardItem, RadioCardLabel, RadioCardRoot } from "../ui/RadioCard";
import { getInlineMessage } from "./inlineMessage";

type RunBackfillFormProps = {
  readonly dag: DAGResponse | DAGWithLatestDagRunsResponse;
  readonly onClose: () => void;
};
const today = new Date().toISOString().slice(0, 16);

type BackfillFormProps = DagRunTriggerParams & Omit<BackfillPostBody, "dag_run_conf">;

const RunBackfillForm = ({ dag, onClose }: RunBackfillFormProps) => {
  const { t: translate } = useTranslation(["components", "common"]);
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

  const inlineMessage = getInlineMessage(isPendingDryRun, affectedTasks.total_entries, translate);

  return (
    <>
      <ErrorAlert error={errors.date ?? error} />
      <VStack alignItems="stretch" gap={2} pt={4}>
        <Box>
          <Text fontSize="md" fontWeight="semibold" mb={3}>
            {translate("backfill.dateRange")}
          </Text>
          <HStack alignItems="flex-start" w="full">
            <Controller
              control={control}
              name="from_date"
              render={({ field }) => (
                <Field.Root invalid={Boolean(errors.date) || dataIntervalInvalid} required>
                  <Field.Label>{translate("common:table.from")}</Field.Label>
                  <DateTimeInput {...field} max={today} onBlur={resetDateError} size="sm" />
                  <Field.ErrorText>{translate("backfill.errorStartDateBeforeEndDate")}</Field.ErrorText>
                </Field.Root>
              )}
            />
            <Controller
              control={control}
              name="to_date"
              render={({ field }) => (
                <Field.Root invalid={Boolean(errors.date) || dataIntervalInvalid} required>
                  <Field.Label>{translate("common:table.to")}</Field.Label>
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
                {translate("backfill.reprocessBehavior")}
              </RadioCardLabel>
              <HStack align="stretch">
                {reprocessBehaviors.map((item) => (
                  <RadioCardItem
                    colorPalette="brand"
                    indicatorPlacement="start"
                    key={item.value}
                    label={translate(item.label)}
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
              <Flex>{translate("backfill.maxRuns")}</Flex>
            </HStack>
          )}
        />
        <Spacer />
        <Controller
          control={control}
          name="run_backwards"
          render={({ field }) => (
            <Checkbox checked={field.value} colorPalette="brand" onChange={field.onChange}>
              {translate("backfill.backwards")}
            </Checkbox>
          )}
        />
        <Spacer />
        {dag.is_paused ? (
          <>
            <Checkbox
              checked={unpause}
              colorPalette="brand"
              onChange={() => setUnpause(!unpause)}
              wordBreak="break-all"
            >
              {translate("backfill.unpause", { dag_display_name: dag.dag_display_name })}
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
          <Button onClick={() => void handleSubmit(onCancel)()}>{translate("common:modal.cancel")}</Button>
          <Button
            colorPalette="brand"
            disabled={
              Boolean(errors.date) || isPendingDryRun || formError || affectedTasks.total_entries === 0
            }
            loading={isPending}
            onClick={() => void handleSubmit(onSubmit)()}
          >
            {translate("backfill.run")}
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default RunBackfillForm;
