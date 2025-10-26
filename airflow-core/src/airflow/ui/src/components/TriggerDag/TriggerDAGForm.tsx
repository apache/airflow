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
  dataIntervalEnd: string;
  dataIntervalStart: string;
  logicalDate: string;
  note: string;
  partitionKey: string | undefined;
};

const TriggerDAGForm = ({ dagDisplayName, dagId, isPaused, onClose, open }: TriggerDAGFormProps) => {
  const { t: translate } = useTranslation(["common", "components"]);
  const { t: rootTranslate } = useTranslation();
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});
  const [formError, setFormError] = useState(false);
  const initialParamsDict = useDagParams(dagId, open);
  const { error: errorTrigger, isPending, triggerDagRun } = useTrigger({ dagId, onSuccessConfirm: onClose });
  const { conf } = useParamStore();
  const [unpause, setUnpause] = useState(true);

  const { mutate: togglePause } = useTogglePause({ dagId });

  const { control, handleSubmit, reset, watch } = useForm<DagRunTriggerParams>({
    defaultValues: {
      conf,
      dagRunId: "",
      dataIntervalEnd: dayjs().startOf('minute').format(DEFAULT_DATETIME_FORMAT),
      dataIntervalStart: dayjs().startOf('minute').format(DEFAULT_DATETIME_FORMAT),
      // Default logical date to now, show it in the selected timezone
      logicalDate: dayjs().format(DEFAULT_DATETIME_FORMAT),
      note: "",
      partitionKey: undefined,
    },
  });

  // Automatically reset form when conf is fetched
  useEffect(() => {
    if (conf) {
      reset((prevValues) => ({
        ...prevValues,
        conf,
      }));
    }
  }, [conf, reset]);

  const resetDateError = () => {
    setErrors((prev) => ({ ...prev, date: undefined }));
  };

  const dataIntervalStart = watch("dataIntervalStart");
  const dataIntervalEnd = watch("dataIntervalEnd");
  const dataIntervalInvalid = dayjs(dataIntervalStart).isAfter(dayjs(dataIntervalEnd));

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
    <Box mt={8}>
      <ConfigForm
        control={control}
        errors={errors}
        initialParamsDict={initialParamsDict}
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
          name="dataIntervalStart"
          render={({ field }) => (
            <Field.Root invalid={Boolean(errors.date) || dataIntervalInvalid} orientation="horizontal">
              <Stack>
                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                  {translate("dagRun.dataIntervalStart")}
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
          name="dataIntervalEnd"
          render={({ field }) => (
            <Field.Root invalid={Boolean(errors.date) || dataIntervalInvalid} orientation="horizontal">
              <Stack>
                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                  {translate("dagRun.dataIntervalEnd")}
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
