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
import { useCallback, useEffect, useRef, useState } from "react";

import { Button, Box, Spacer, HStack, Field, Stack, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { Controller, useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";

import { useDagServiceGetDagDetails } from "openapi/queries";

import { RadioCardItem, RadioCardRoot } from "src/system-components";

import { useTimezone } from "src/context/timezone";
import { useDagParams } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";
import { useTogglePause } from "src/queries/useTogglePause";
import { DATE_FORMAT, DEFAULT_DATETIME_FORMAT, formatDate } from "src/utils/datetimeUtils";

import ConfigForm from "../ConfigForm";
import { DateTimeInput } from "../DateTimeInput";
import { ErrorAlert, type ExpandedApiError } from "../ErrorAlert";
import PausedDagOptions from "./PausedDagOptions";
import RecentConfigSelect from "./RecentConfigSelect";
import TriggerDAGAdvancedOptions from "./TriggerDAGAdvancedOptions";
import {
  dataIntervalModeOptions,
  isDataIntervalMode,
  type DagRunTriggerParams,
  type PausedDagAction,
} from "./types";

type TriggerDAGFormProps = {
  readonly dagId: string;
  readonly disabled?: boolean;
  readonly error?: unknown;
  readonly hasSchedule: boolean;
  readonly isPartitioned: boolean;
  readonly isPartitionedAtRuntime: boolean;
  readonly isPaused: boolean;
  readonly isPending?: boolean;
  readonly onPausedDagActionChange?: () => void;
  readonly onSubmitTrigger?: (params: DagRunTriggerParams) => void;
  readonly open: boolean;
  readonly prefillConfig?:
    | {
        conf: Record<string, unknown> | undefined;
        logicalDate: string | undefined;
        runId: string;
      }
    | undefined;
  readonly suggestedPartitionKey?: string | null | undefined;
};

const DAG_DATE_FORMAT = `${DATE_FORMAT} HH:mm z`;

const TriggerDAGForm = ({
  dagId,
  disabled = false,
  error,
  hasSchedule,
  isPartitioned,
  isPartitionedAtRuntime,
  isPaused,
  isPending = false,
  onPausedDagActionChange,
  onSubmitTrigger,
  open,
  prefillConfig,
  suggestedPartitionKey,
}: TriggerDAGFormProps) => {
  const { t: translate } = useTranslation(["common", "components"]);
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});
  const [formError, setFormError] = useState(false);
  const initialParamsDict = useDagParams(dagId, open);
  const { data: dagDetails } = useDagServiceGetDagDetails({ dagId }, undefined, { enabled: open });
  const { selectedTimezone } = useTimezone();
  const { conf, initialParamDict, setConf, setInitialParamDict } = useParamStore();
  const [pausedDagAction, setPausedDagAction] = useState<PausedDagAction>("unpause");
  const [hasAppliedPrefill, setHasAppliedPrefill] = useState(false);
  const { mutate: togglePause } = useTogglePause({ dagId });
  const showsPartitionKey = isPartitioned || isPartitionedAtRuntime;
  // Only a Dag that shows the partition key field takes a suggestion; for any other Dag it is ignored.
  const partitionKeySuggestion = showsPartitionKey ? suggestedPartitionKey : undefined;

  const { control, handleSubmit, reset, setValue, watch } = useForm<DagRunTriggerParams>({
    defaultValues: {
      conf,
      dagRunId: "",
      dataIntervalEnd: "",
      dataIntervalMode: "auto",
      dataIntervalStart: "",
      // Default logical date to now, show it in the selected timezone.
      // For partitioned Dags, logical date is not applicable.
      logicalDate: isPartitioned ? "" : dayjs().format(DEFAULT_DATETIME_FORMAT),
      note: "",
      partitionKey: partitionKeySuggestion ?? undefined,
    },
  });

  // A ref rather than RHF's dirtyFields: reset(updater) makes the current values the new defaults
  // and clears dirty state, so dirtyFields forgets an edit as soon as the conf changes.
  const userEditedPartitionKeyRef = useRef(false);
  // The suggestion last written into the field by the form itself; "" once the user edits the field.
  // State, not a ref, because the "suggested value" helper text is derived from it.
  const [appliedSuggestion, setAppliedSuggestion] = useState(partitionKeySuggestion ?? "");
  // Read by the prefill effect, which must not depend on the suggestion: a new suggestion would
  // otherwise reset the whole form and discard what the user typed.
  const latestSuggestionRef = useRef(partitionKeySuggestion);

  // Must be declared before the prefill effect: effects of one commit run in declaration order, and
  // the prefill effect has to read the suggestion of this commit.
  useEffect(() => {
    latestSuggestionRef.current = partitionKeySuggestion;
  }, [partitionKeySuggestion]);

  // Push a conf into the param store. Seed the initial params only once they are available,
  // but always push the conf so a run's configuration propagates even
  // for Dags with no declared params or before params load.
  const syncConfToStore = useCallback(
    (confString: string) => {
      if (
        Object.keys(initialParamsDict.paramsDict).length > 0 &&
        Object.keys(initialParamDict).length === 0
      ) {
        setInitialParamDict(initialParamsDict.paramsDict);
      }
      setConf(confString);
    },
    [initialParamDict, initialParamsDict.paramsDict, setConf, setInitialParamDict],
  );

  // Selecting a recent configuration mid-edit only swaps the conf; whatever the user already
  // typed into logical date, run id, note or data interval is kept.
  const applyRecentConf = useCallback(
    (confObj: Record<string, unknown>) => {
      const confString = JSON.stringify(confObj, undefined, 2);

      reset((prevValues) => ({ ...prevValues, conf: confString }));
      syncConfToStore(confString);
    },
    [reset, syncConfToStore],
  );

  // Pre-fill form when prefillConfig is provided. This runs on open, before any user input, so
  // the whole form is reset. Only 'conf' is copied from the prior run; its runId and logicalDate
  // are deliberately dropped, because POST /dagRuns rejects a run whose id or logical date
  // already exists with 409 Conflict. The partition key is dropped for the same reason and
  // seeded from the current suggestion instead.
  useEffect(() => {
    if (prefillConfig && open) {
      const confString = prefillConfig.conf ? JSON.stringify(prefillConfig.conf, undefined, 2) : "";
      const latestSuggestion = latestSuggestionRef.current ?? "";

      reset({
        conf: confString,
        dagRunId: "",
        dataIntervalEnd: "",
        dataIntervalMode: "auto",
        dataIntervalStart: "",
        logicalDate: isPartitioned ? "" : dayjs().format(DEFAULT_DATETIME_FORMAT),
        note: "",
        partitionKey: latestSuggestion,
      });
      // The reset discards the user's partition key too, so the field follows the suggestion again.
      userEditedPartitionKeyRef.current = false;
      setAppliedSuggestion(latestSuggestion);
      if (confString) {
        syncConfToStore(confString);
      }
      setHasAppliedPrefill(true);
    } else if (!open) {
      setHasAppliedPrefill(false);
      userEditedPartitionKeyRef.current = false;
      setAppliedSuggestion("");
    }
  }, [prefillConfig, open, isPartitioned, reset, syncConfToStore]);

  // The suggestion is computed per request, so it can arrive late or change while the form is open.
  // Follow it until the user edits the field; clearing the field counts as an edit.
  useEffect(() => {
    if (!open || userEditedPartitionKeyRef.current || partitionKeySuggestion === undefined) {
      return;
    }
    // A null suggestion means "none": the field is emptied.
    const suggestion = partitionKeySuggestion ?? "";

    setAppliedSuggestion(suggestion);
    setValue("partitionKey", suggestion);
  }, [open, partitionKeySuggestion, setValue]);

  const onPartitionKeyEdit = useCallback(() => {
    userEditedPartitionKeyRef.current = true;
    setAppliedSuggestion("");
  }, []);

  // Automatically reset form when conf is fetched (only if no prefillConfig)
  useEffect(() => {
    if (conf && open && (!prefillConfig || hasAppliedPrefill)) {
      reset((prevValues) => ({ ...prevValues, conf }));
    }
  }, [conf, hasAppliedPrefill, prefillConfig, open, reset]);

  const resetDateError = () => setErrors((prev) => ({ ...prev, date: undefined }));

  const logicalDate = watch("logicalDate");
  const partitionKey = watch("partitionKey");
  const isSuggestedValue = Boolean(partitionKey) && partitionKey === appliedSuggestion;
  const startDate = dagDetails?.start_date ?? undefined;
  const endDate = dagDetails?.end_date ?? undefined;
  const formatDagDate = (date: string) => formatDate(date, selectedTimezone, DAG_DATE_FORMAT);
  let dagStartEndDate: string | undefined;

  if (startDate !== undefined && endDate !== undefined) {
    dagStartEndDate = translate("components:triggerDag.dagStartEndDate.both", {
      endDate: formatDagDate(endDate),
      startDate: formatDagDate(startDate),
    });
  } else if (startDate !== undefined) {
    dagStartEndDate = translate("components:triggerDag.dagStartEndDate.start", {
      startDate: formatDagDate(startDate),
    });
  } else if (endDate !== undefined) {
    dagStartEndDate = translate("components:triggerDag.dagStartEndDate.end", {
      endDate: formatDagDate(endDate),
    });
  }
  // Every task's start and end dates are bounded by the Dag's, so a run outside them creates no
  // task instances and is marked successful without running anything.
  const isOutsideDagStartEndDate =
    Boolean(logicalDate) &&
    ((startDate !== undefined && dayjs(logicalDate).isBefore(startDate)) ||
      (endDate !== undefined && dayjs(logicalDate).isAfter(endDate)));

  const dataIntervalMode = watch("dataIntervalMode");
  const dataIntervalStart = watch("dataIntervalStart");
  const dataIntervalEnd = watch("dataIntervalEnd");
  const noDataInterval = !Boolean(dataIntervalStart) || !Boolean(dataIntervalEnd);
  const dataIntervalInvalid =
    dataIntervalMode === "manual" &&
    (noDataInterval || dayjs(dataIntervalStart).isAfter(dayjs(dataIntervalEnd)));
  const onSubmit = (data: DagRunTriggerParams) => {
    if (disabled) {
      return;
    }
    if (pausedDagAction === "unpause" && isPaused) {
      togglePause({ dagId, requestBody: { is_paused: false } });
    }
    onSubmitTrigger?.({ ...data, drainDag: isPaused && pausedDagAction === "drain" });
  };

  return (
    <>
      <ErrorAlert error={errors.date ?? error} />
      <VStack alignItems="stretch" gap={2} pt={4}>
        {isPartitioned ? undefined : (
          <>
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
                    {dagStartEndDate === undefined ? undefined : (
                      <Field.HelperText color={isOutsideDagStartEndDate ? "fg.warning" : undefined}>
                        {isOutsideDagStartEndDate
                          ? `${translate("components:triggerDag.dagStartEndDate.noTaskWillRun")} ${dagStartEndDate}`
                          : dagStartEndDate}
                      </Field.HelperText>
                    )}
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
                    <RadioCardRoot
                      onValueChange={(details) => {
                        if (isDataIntervalMode(details.value)) {
                          field.onChange(details.value);
                        }
                      }}
                      value={field.value}
                    >
                      <HStack align="stretch">
                        {dataIntervalModeOptions.map((mode) => (
                          <RadioCardItem
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
          </>
        )}
        {isPaused ? (
          <>
            <PausedDagOptions
              dagId={dagId}
              onChange={(action) => {
                setPausedDagAction(action);
                onPausedDagActionChange?.();
              }}
              value={pausedDagAction}
            />
            <Spacer />
          </>
        ) : undefined}
        {prefillConfig ? undefined : (
          <RecentConfigSelect dagId={dagId} onSelectConf={applyRecentConf} open={open} />
        )}
        <ConfigForm
          control={control}
          errors={errors}
          initialParamsDict={initialParamsDict}
          setErrors={setErrors}
          setFormError={setFormError}
        >
          <TriggerDAGAdvancedOptions
            control={control}
            isPartitioned={isPartitioned}
            isPartitionedAtRuntime={isPartitionedAtRuntime}
            isSuggestedValue={isSuggestedValue}
            onPartitionKeyEdit={onPartitionKeyEdit}
          />
        </ConfigForm>
      </VStack>
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          <Spacer />
          <Button
            data-testid="trigger-dag-submit"
            disabled={
              disabled ||
              Boolean(errors.conf) ||
              Boolean(errors.date) ||
              formError ||
              isPending ||
              dataIntervalInvalid ||
              (Boolean(error) && (error as ExpandedApiError).status === 403)
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
