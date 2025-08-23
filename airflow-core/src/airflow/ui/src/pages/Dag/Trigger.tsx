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
import { Box, Heading, VStack, Spinner, Center, Text, Button } from "@chakra-ui/react";
import { useSearchParams, useNavigate, useParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { FiArrowLeft } from "react-icons/fi";

import { useDagServiceGetDag } from "openapi/queries";
import { useDagParams } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";
import { useTogglePause } from "src/queries/useTogglePause";
import { useTrigger } from "src/queries/useTrigger";

import ConfigForm from "src/components/ConfigForm";
import { DateTimeInput } from "src/components/DateTimeInput";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Checkbox } from "src/components/ui/Checkbox";
import EditableMarkdown from "src/components/TriggerDag/EditableMarkdown";
import { Field, Input, Stack } from "src/components/ui";
import { Controller, useForm } from "react-hook-form";
import dayjs from "dayjs";
import { useEffect, useState } from "react";

export type DagRunTriggerParams = {
  conf: string;
  dagRunId: string;
  logicalDate: string;
  note: string;
};

export const Trigger = () => {
  const { t: translate } = useTranslation(["common", "components"]);
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { dagId = "" } = useParams();
  const [errors, setErrors] = useState<{ conf?: string; date?: unknown }>({});
  const [formError, setFormError] = useState(false);
  const initialParamsDict = useDagParams(dagId, true);
  const { error: errorTrigger, isPending, triggerDagRun } = useTrigger({ 
    dagId, 
    onSuccessConfirm: () => navigate(`/dags/${dagId}`) 
  });
  const { conf } = useParamStore();
  const [unpause, setUnpause] = useState(true);

  const { mutate: togglePause } = useTogglePause({ dagId });

  const {
    data: dag,
    isError,
    isLoading,
  } = useDagServiceGetDag(
    {
      dagId,
    },
    undefined,
    {
      enabled: Boolean(dagId),
    },
  );

  // Get URL parameters for pre-population
  const urlConf = searchParams.get("conf");
  const urlDagRunId = searchParams.get("dag_run_id");
  const urlLogicalDate = searchParams.get("logical_date");
  const urlNote = searchParams.get("note");

  const { control, handleSubmit, reset } = useForm<DagRunTriggerParams>({
    defaultValues: {
      conf: urlConf || conf,
      dagRunId: urlDagRunId || "",
      logicalDate: urlLogicalDate || dayjs().format("YYYY-MM-DDTHH:mm:ss.SSS"),
      note: urlNote || "",
    },
  });

  // Automatically reset form when conf is fetched or URL params change
  useEffect(() => {
    if (conf || urlConf) {
      reset((prevValues) => ({
        ...prevValues,
        conf: urlConf || conf,
        dagRunId: urlDagRunId || prevValues.dagRunId,
        logicalDate: urlLogicalDate || prevValues.logicalDate,
        note: urlNote || prevValues.note,
      }));
    }
  }, [conf, urlConf, urlDagRunId, urlLogicalDate, urlNote, reset]);

  const resetDateError = () => {
    setErrors((prev) => ({ ...prev, date: undefined }));
  };

  const onSubmit = (data: DagRunTriggerParams) => {
    if (unpause && dag?.is_paused) {
      togglePause({
        dagId,
        requestBody: {
          is_paused: false,
        },
      });
    }
    triggerDagRun(data);
  };

  const handleCancel = () => {
    navigate(`/dags/${dagId}`);
  };

  if (isLoading) {
    return (
      <Center py={6}>
        <VStack>
          <Spinner size="lg" />
          <Text mt={2}>{translate("components:triggerDag.loading")}</Text>
        </VStack>
      </Center>
    );
  }

  if (isError || !dag) {
    return (
      <Center py={6}>
        <Text color="red.500">{translate("components:triggerDag.loadingFailed")}</Text>
      </Center>
    );
  }

  const maxDisplayLength = 59;
  const nameOverflowing = dag.dag_display_name.length > maxDisplayLength;

  return (
    <Box m={4} maxW="800px" mx="auto">
      <VStack align="start" gap={4} width="100%">
        <Button
          leftIcon={<FiArrowLeft />}
          onClick={handleCancel}
          variant="ghost"
        >
          {translate("common:back")}
        </Button>
        
        <Heading size="xl" wordBreak="break-all">
          {translate("components:triggerDag.title")} - {nameOverflowing ? <br /> : undefined} {dag.dag_display_name}
        </Heading>

        <Box as="form" onSubmit={handleSubmit(onSubmit)} width="100%">
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
              name="note"
              render={({ field }) => (
                <Field.Root mt={6}>
                  <Field.Label fontSize="md">{translate("note.dagRun")}</Field.Label>
                  <EditableMarkdown field={field} placeholder={translate("note.placeholder")} />
                </Field.Root>
              )}
            />
          </ConfigForm>
          
          {dag.is_paused ? (
            <Checkbox
              checked={unpause}
              colorPalette="blue"
              onChange={() => setUnpause(!unpause)}
              wordBreak="break-all"
              mt={4}
            >
              {translate("components:triggerDag.unpause", { dagDisplayName: dag.dag_display_name })}
            </Checkbox>
          ) : undefined}
          
          <ErrorAlert error={errors.date ?? errorTrigger} />
          
          <Box as="footer" display="flex" justifyContent="flex-end" mt={6} gap={3}>
            <Button onClick={handleCancel} variant="outline">
              {translate("common:cancel")}
            </Button>
            <Button
              colorPalette="blue"
              disabled={Boolean(errors.conf) || Boolean(errors.date) || formError || isPending}
              type="submit"
            >
              {translate("components:triggerDag.button")}
            </Button>
          </Box>
        </Box>
      </VStack>
    </Box>
  );
};
