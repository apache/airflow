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
import { Button, Box, Spacer, HStack, Field, Stack, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useEffect, useState } from "react";
import { Controller, useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";

import { useDagParams } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";
import { useTogglePause } from "src/queries/useTogglePause";
import { useTrigger } from "src/queries/useTrigger";
import { DEFAULT_DATETIME_FORMAT } from "src/utils/datetimeUtils";

import ConfigForm from "../ConfigForm";
import { DateTimeInput } from "../DateTimeInput";
import { ErrorAlert, type ExpandedApiError } from "../ErrorAlert";
import { Checkbox } from "../ui/Checkbox";
import { RadioCardItem, RadioCardRoot } from "../ui/RadioCard";
import TriggerDAGAdvancedOptions from "./TriggerDAGAdvancedOptions";
import type { DagRunTriggerParams } from "./types";
import { dataIntervalModeOptions } from "./types";

type TriggerDAGFormProps = {
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly hasSchedule: boolean;
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

const TriggerDAGForm = ({
  dagDisplayName,
  dagId,
  hasSchedule,
  isPaused,
  onClose,
  open,
  prefillConfig,
}: TriggerDAGFormProps) => {
  const { t: translate } = useTranslation(["common", "components"]);
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});
  const [formError, setFormError] = useState(false);
  const initialParamsDict = useDagParams(dagId, open);
  const { error: errorTrigger, isPending, triggerDagRun } = useTrigger({ dagId, onSuccessConfirm: onClose });
  const { conf, initialParamDict, setConf, setInitialParamDict } = useParamStore();
  const [unpause, setUnpause] = useState(true);

  const { mutate: togglePause } = useTogglePause({ dagId });

  const { control, handleSubmit, reset, watch } = useForm<DagRunTriggerParams>({
    defaultValues: {
      conf,
      dagRunId: "",
      dataIntervalEnd: "",
      dataIntervalMode: "auto",
      dataIntervalStart: "",
      // Default logical date to now, show it in the selected timezone
      logicalDate: dayjs().format(DEFAULT_DATETIME_FORMAT),
      note: "",
      partitionKey: undefined,
    },
  });

  // Pre-fill form when prefillConfig is provided (priority over conf)
  // Only restore 'conf' (parameters), not logicalDate, runId, or partitionKey to avoid 409 conflicts
  useEffect(() => {
    if (prefillConfig && open) {
      const confString = prefillConfig.conf ? JSON.stringify(prefillConfig.conf, undefined, 2) : "";

      reset({
        conf: confString,
        dagRunId: "",
        dataIntervalEnd: "",
        dataIntervalMode: "auto",
        dataIntervalStart: "",
        logicalDate: dayjs().format(DEFAULT_DATETIME_FORMAT),
        note: "",
        partitionKey: undefined,
      });

      // Also update the param store to keep it in sync.
      // Wait until we have the initial params so section ordering stays consistent.
      if (confString && Object.keys(initialParamsDict.paramsDict).length > 0) {
        if (Object.keys(initialParamDict).length === 0) {
          setInitialParamDict(initialParamsDict.paramsDict);
        }
        setConf(confString);
      }
    }
  }, [
    prefillConfig,
    open,
    reset,
    setConf,
    initialParamsDict.paramsDict,
    initialParamDict,
    setInitialParamDict,
  ]);

  // Automatically reset form when conf is fetched (only if no prefillConfig)
  useEffect(() => {
    if (conf && !prefillConfig && open) {
      reset((prevValues) => ({
        ...prevValues,
        conf,
      }));
    }
  }, [conf, prefillConfig, open, reset]);

  const resetDateError = () => {
    setErrors((prev) => ({ ...prev, date: undefined }));
  };

  const dataIntervalMode = watch("dataIntervalMode");
  const dataIntervalStart = watch("dataIntervalStart");
  const dataIntervalEnd = watch("dataIntervalEnd");
  const noDataInterval = !Boolean(dataIntervalStart) || !Boolean(dataIntervalEnd);
  const dataIntervalInvalid =
    dataIntervalMode === "manual" &&
    (noDataInterval || dayjs(dataIntervalStart).isAfter(dayjs(dataIntervalEnd)));
  const onSubmit = (data: DagRunTriggerParams) => {
    if (unpause && isPaused) {
      togglePause({
        dagId,
        requestBody: {
          is_paused: false,
        },
      });
    }
    triggerDagRun(data);
  };

  return (
    <>
      <ErrorAlert error={errors.date ?? errorTrigger} />
      <VStack alignItems="stretch" gap={2} pt={4}>
        <Controller
          control={control}
          name="logicalDate"
          render={({ field }) => (
            <Field.Root invalid={Boolean(errors.date)} orientation="horizontal">
              <Stack>
                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                  {translate("logicalDate")}
                </Field.Label>
              </Stack>
              <Stack css={{ flexBasis: "70%" }}>
                <DateTimeInput {...field} onBlur={resetDateError} size="sm" />
              </Stack>
            </Field.Root>
          )}
        />
        <Spacer />
        {hasSchedule ? (
          <Box>
            <Text fontSize="md" fontWeight="semibold" mb={3}>
              {translate("components:triggerDag.dataInterval")}
            </Text>
            <Controller
              control={control}
              name="dataIntervalMode"
              render={({ field }) => (
                <RadioCardRoot defaultValue={String(field.value)} onChange={field.onChange}>
                  <HStack align="stretch">
                    {dataIntervalModeOptions.map((mode) => (
                      <RadioCardItem
                        colorPalette="brand"
                        indicatorPlacement="start"
                        key={mode.value}
                        label={translate(mode.label)}
                        value={mode.value}
                      />
                    ))}
                  </HStack>
                </RadioCardRoot>
              )}
            />
            <Spacer />
            {dataIntervalMode === "manual" ? (
              <HStack alignItems="flex-start" mt={3} w="full">
                <Controller
                  control={control}
                  name="dataIntervalStart"
                  render={({ field }) => (
                    <Field.Root invalid={Boolean(errors.date) || dataIntervalInvalid} required>
                      <Field.Label>{translate("components:triggerDag.intervalStart")}</Field.Label>
                      <DateTimeInput {...field} onBlur={resetDateError} size="sm" />
                    </Field.Root>
                  )}
                />
                <Controller
                  control={control}
                  name="dataIntervalEnd"
                  render={({ field }) => (
                    <Field.Root invalid={Boolean(errors.date) || dataIntervalInvalid} required>
                      <Field.Label>{translate("components:triggerDag.intervalEnd")}</Field.Label>
                      <DateTimeInput {...field} onBlur={resetDateError} size="sm" />
                    </Field.Root>
                  )}
                />
              </HStack>
            ) : undefined}
          </Box>
        ) : undefined}
        <Spacer />
        {isPaused ? (
          <>
            <Checkbox
              checked={unpause}
              colorPalette="brand"
              onChange={() => setUnpause(!unpause)}
              wordBreak="break-all"
            >
              {translate("components:triggerDag.unpause", { dagDisplayName })}
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
        >
          <TriggerDAGAdvancedOptions control={control} />
        </ConfigForm>
      </VStack>
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          <Spacer />
          <Button
            colorPalette="brand"
            disabled={
              Boolean(errors.conf) ||
              Boolean(errors.date) ||
              formError ||
              isPending ||
              dataIntervalInvalid ||
              (Boolean(errorTrigger) && (errorTrigger as ExpandedApiError).status === 403)
            }
            onClick={() => void handleSubmit(onSubmit)()}
          >
            <FiPlay /> {translate("components:triggerDag.button")}
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default TriggerDAGForm;
