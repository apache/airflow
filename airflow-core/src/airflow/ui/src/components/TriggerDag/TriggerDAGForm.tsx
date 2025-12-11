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
import { Button, Box, Spacer, HStack, Input, Field, Stack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useEffect, useState, useRef, useMemo } from "react";
import { Controller, useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";
import { useSearchParams } from "react-router-dom";

import { useDagParams } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";
import { useTogglePause } from "src/queries/useTogglePause";
import { useTrigger } from "src/queries/useTrigger";
import { DEFAULT_DATETIME_FORMAT } from "src/utils/datetimeUtils";
import { getTriggerConf } from "src/utils/trigger";

import ConfigForm from "../ConfigForm";
import { DateTimeInput } from "../DateTimeInput";
import { ErrorAlert } from "../ErrorAlert";
import { Checkbox } from "../ui/Checkbox";
import EditableMarkdown from "./EditableMarkdown";

type TriggerDAGFormProps = {
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly isPaused: boolean;
  readonly onClose: () => void;
  readonly open: boolean;
};

export type DagRunTriggerParams = {
  conf: string;
  dagRunId: string;
  logicalDate: string;
  note: string;
  params?: Record<string, unknown>;
  partitionKey: string | undefined;
};

const extractParamValues = (obj: Record<string, unknown>) => {
  const out: Record<string, unknown> = {};

  Object.entries(obj).forEach(([key, val]) => {
    if (val !== null && typeof val === "object" && "value" in val) {
      out[key] = (val as { value: unknown }).value;
    } else if (val !== null && typeof val === "object" && "default" in val) {
      out[key] = (val as { default: unknown }).default;
    } else {
      out[key] = val;
    }
  });

  return out;
};

const TriggerDAGForm = ({ dagDisplayName, dagId, isPaused, onClose, open }: TriggerDAGFormProps) => {
  const { t: translate } = useTranslation(["common", "components"]);
  const { t: rootTranslate } = useTranslation();
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});
  const [formError, setFormError] = useState(false);
  const initialParamsDict = useDagParams(dagId, open);
  const { error: errorTrigger, isPending, triggerDagRun } = useTrigger({ dagId, onSuccessConfirm: onClose });
  const { conf, setParamsDict } = useParamStore();
  const [unpause, setUnpause] = useState(true);
  const [searchParams] = useSearchParams();
  const urlConf = getTriggerConf(searchParams, ["run_id", "logical_date", "note"]);
  const urlRunId = searchParams.get("run_id") ?? "";
  const urlDate = searchParams.get("logical_date");
  const urlNote = searchParams.get("note") ?? "";

  const { mutate: togglePause } = useTogglePause({ dagId });

  const defaultsRef = useRef<DagRunTriggerParams | undefined>(undefined);
  const isSyncedRef = useRef(false);

  const cleanInitialParams = useMemo(
    () => extractParamValues(initialParamsDict.paramsDict as Record<string, unknown>),
    [initialParamsDict.paramsDict],
  );
  const { control, getValues, handleSubmit, reset } = useForm<DagRunTriggerParams>({
    defaultValues: {
      ...initialParamsDict,
      conf: urlConf === "{}" ? conf || "{}" : urlConf,
      dagRunId: urlRunId,
      logicalDate: urlDate ?? dayjs().format(DEFAULT_DATETIME_FORMAT),
      note: urlNote,
      params: cleanInitialParams,
      partitionKey: undefined,
    },
  });

  // Automatically reset form when conf is fetched
  useEffect(() => {
    if (defaultsRef.current === undefined && Object.keys(cleanInitialParams).length > 0) {
      const current = getValues();

      defaultsRef.current = {
        ...current,
        params: cleanInitialParams,
      };
    }
  }, [getValues, cleanInitialParams]);

  useEffect(() => {
    if (defaultsRef.current === undefined) {
      return;
    }

    if (isSyncedRef.current) {
      return;
    }

    if (urlConf === "{}") {
      if (conf) {
        reset((prev) => ({ ...prev, conf }));
      }
      isSyncedRef.current = true;

      return;
    }

    let parsed: Record<string, unknown> = {};

    try {
      parsed = JSON.parse(urlConf) as Record<string, unknown>;
    } catch {
      /* empty */
    }
    const mergedValues = { ...defaultsRef.current.params, ...parsed };
    const mergedConfJson = JSON.stringify(mergedValues, undefined, 2);

    reset({
      ...defaultsRef.current,
      conf: mergedConfJson,
      dagRunId: Boolean(urlRunId) ? urlRunId : defaultsRef.current.dagRunId,
      logicalDate: urlDate ?? defaultsRef.current.logicalDate,
      note: Boolean(urlNote) ? urlNote : defaultsRef.current.note,
      partitionKey: undefined,
    });

    const updatedParamsDict = structuredClone(initialParamsDict.paramsDict);

    Object.entries(mergedValues).forEach(([key, val]) => {
      if (updatedParamsDict[key]) {
        updatedParamsDict[key].value = val;
      }
    });
    setParamsDict(updatedParamsDict);

    isSyncedRef.current = true;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlConf, urlRunId, urlDate, urlNote, initialParamsDict, reset, setParamsDict]);

  const resetDateError = () => setErrors((prev) => ({ ...prev, date: undefined }));

  const onSubmit = (data: DagRunTriggerParams) => {
    if (unpause && isPaused) {
      togglePause({ dagId, requestBody: { is_paused: false } });
    }

    const finalParams = { ...data.params };

    try {
      const manualJson = JSON.parse(data.conf) as Record<string, unknown>;

      Object.assign(finalParams, manualJson);
    } catch {
      /* empty */
    }

    triggerDagRun({
      ...data,
      conf: JSON.stringify(finalParams),
    });
  };

  return (
    <Box mt={8}>
      <ConfigForm
        control={control}
        errors={errors}
        initialParamsDict={initialParamsDict}
        openAdvanced={urlConf !== "{}" || Boolean(urlRunId) || Boolean(urlDate) || Boolean(urlNote)}
        setErrors={setErrors}
        setFormError={setFormError}
      >
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

        <Controller
          control={control}
          name="dagRunId"
          render={({ field }) => (
            <Field.Root mt={6} orientation="horizontal">
              <Stack>
                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                  {translate("runId")}
                </Field.Label>
              </Stack>
              <Stack css={{ flexBasis: "70%" }}>
                <Input {...field} size="sm" />
                <Field.HelperText>{translate("components:triggerDag.runIdHelp")}</Field.HelperText>
              </Stack>
            </Field.Root>
          )}
        />
        <Controller
          control={control}
          name="partitionKey"
          render={({ field }) => (
            <Field.Root mt={6} orientation="horizontal">
              <Stack>
                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                  {rootTranslate("dagRun.partitionKey")}
                </Field.Label>
              </Stack>
              <Stack css={{ flexBasis: "70%" }}>
                <Input {...field} size="sm" />
              </Stack>
            </Field.Root>
          )}
        />
        <Controller
          control={control}
          name="note"
          render={({ field }) => (
            <Field.Root mt={6}>
              <Field.Label fontSize="md">{translate("note.dagRun")}</Field.Label>
              <EditableMarkdown field={field} placeholder={translate("note.placeholder")} />
            </Field.Root>
          )}
        />
      </ConfigForm>
      {isPaused ? (
        <Checkbox
          checked={unpause}
          colorPalette="brand"
          onChange={() => setUnpause(!unpause)}
          wordBreak="break-all"
        >
          {translate("components:triggerDag.unpause", { dagDisplayName })}
        </Checkbox>
      ) : undefined}
      <ErrorAlert error={errors.date ?? errorTrigger} />
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          <Spacer />
          <Button
            colorPalette="brand"
            disabled={
              Boolean(errors.conf) || Boolean(errors.date) || formError || isPending || Boolean(errorTrigger)
            }
            onClick={() => void handleSubmit(onSubmit)()}
          >
            <FiPlay /> {translate("components:triggerDag.button")}
          </Button>
        </HStack>
      </Box>
    </Box>
  );
};

export default TriggerDAGForm;
