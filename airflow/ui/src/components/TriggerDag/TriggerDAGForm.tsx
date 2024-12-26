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
import { Input, Button, Box, Spacer, HStack, Field } from "@chakra-ui/react";
import { json } from "@codemirror/lang-json";
import { githubLight, githubDark } from "@uiw/codemirror-themes-all";
import CodeMirror from "@uiw/react-codemirror";
import { useState } from "react";
import { useForm, Controller } from "react-hook-form";
import { FiPlay } from "react-icons/fi";

import { useColorMode } from "src/context/colorMode";
import { useDagParams } from "src/queries/useDagParams";
import { useTrigger } from "src/queries/useTrigger";

import { ErrorAlert } from "../ErrorAlert";
import { Accordion } from "../ui";

type TriggerDAGFormProps = {
  dagId: string;
  onClose: () => void;
  open: boolean;
};

export type DagRunTriggerParams = {
  conf: string;
  dagRunId: string;
  dataIntervalEnd: string;
  dataIntervalStart: string;
  note: string;
};

const TriggerDAGForm: React.FC<TriggerDAGFormProps> = ({
  dagId,
  onClose,
  open,
}) => {
  const [errors, setErrors] = useState<{ conf?: string; date?: string }>({});
  const conf = useDagParams(dagId, open);
  const { error: errorTrigger, isPending, triggerDagRun } = useTrigger(onClose);

  const {
    control,
    formState: { isDirty },
    handleSubmit,
    reset,
    setValue,
    watch,
  } = useForm<DagRunTriggerParams>({
    defaultValues: {
      conf,
      dagRunId: "",
      dataIntervalEnd: "",
      dataIntervalStart: "",
      note: "",
    },
  });

  const dataIntervalStart = watch("dataIntervalStart");
  const dataIntervalEnd = watch("dataIntervalEnd");

  const handleReset = () => {
    setErrors({ conf: undefined, date: undefined });
    reset();
  };

  const validateAndPrettifyJson = (value: string) => {
    try {
      const parsedJson = JSON.parse(value) as JSON;

      setErrors((prev) => ({ ...prev, conf: undefined }));

      return JSON.stringify(parsedJson, undefined, 2);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error occurred.";

      setErrors((prev) => ({
        ...prev,
        conf: `Invalid JSON format: ${errorMessage}`,
      }));

      return value;
    }
  };

  const onSubmit = (data: DagRunTriggerParams) => {
    if (Boolean(data.dataIntervalStart) !== Boolean(data.dataIntervalEnd)) {
      setErrors((prev) => ({
        ...prev,
        date: "Either both Data Interval Start and End must be provided, or both must be empty.",
      }));

      return;
    }
    triggerDagRun(dagId, data);
  };

  const validateDates = (
    fieldName: "dataIntervalEnd" | "dataIntervalStart",
  ) => {
    const startDate = dataIntervalStart
      ? new Date(dataIntervalStart)
      : undefined;
    const endDate = dataIntervalEnd ? new Date(dataIntervalEnd) : undefined;

    setErrors((prev) => ({ ...prev, date: undefined }));

    if (startDate && endDate) {
      if (fieldName === "dataIntervalStart" && startDate > endDate) {
        setValue("dataIntervalStart", dataIntervalEnd);
      } else if (fieldName === "dataIntervalEnd" && endDate < startDate) {
        setValue("dataIntervalEnd", dataIntervalStart);
      }
    }
  };

  const { colorMode } = useColorMode();

  return (
    <>
      <Accordion.Root collapsible mb={4} mt={4} size="lg" variant="enclosed">
        <Accordion.Item key="advancedOptions" value="advancedOptions">
          <Accordion.ItemTrigger cursor="button">
            Advanced Options
          </Accordion.ItemTrigger>
          <Accordion.ItemContent>
            <Box p={5}>
              <Controller
                control={control}
                name="dataIntervalStart"
                render={({ field }) => (
                  <Field.Root invalid={Boolean(errors.date)}>
                    <Field.Label fontSize="md">
                      Data Interval Start Date
                    </Field.Label>
                    <Input
                      {...field}
                      max={dataIntervalEnd || undefined}
                      onBlur={() => validateDates("dataIntervalStart")}
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
                  <Field.Root invalid={Boolean(errors.date)} mt={6}>
                    <Field.Label fontSize="md">
                      Data Interval End Date
                    </Field.Label>
                    <Input
                      {...field}
                      min={dataIntervalStart || undefined}
                      onBlur={() => validateDates("dataIntervalEnd")}
                      placeholder="yyyy-mm-ddThh:mm"
                      size="sm"
                      type="datetime-local"
                    />
                    {Boolean(errors.date) ? (
                      <Field.ErrorText>{errors.date}</Field.ErrorText>
                    ) : undefined}
                  </Field.Root>
                )}
              />

              <Controller
                control={control}
                name="dagRunId"
                render={({ field }) => (
                  <Field.Root mt={6}>
                    <Field.Label fontSize="md">Run ID</Field.Label>
                    <Input
                      {...field}
                      placeholder="Run Id, optional - will be generated if not provided"
                      size="sm"
                    />
                  </Field.Root>
                )}
              />

              <Controller
                control={control}
                name="conf"
                render={({ field }) => (
                  <Field.Root invalid={Boolean(errors.conf)} mt={6}>
                    <Field.Label fontSize="md">Configuration JSON</Field.Label>
                    <CodeMirror
                      {...field}
                      basicSetup={{
                        autocompletion: true,
                        bracketMatching: true,
                        foldGutter: true,
                        lineNumbers: true,
                      }}
                      extensions={[json()]}
                      height="200px"
                      onBlur={() => {
                        const prettifiedJson = validateAndPrettifyJson(
                          field.value,
                        );

                        field.onChange(prettifiedJson);
                      }}
                      style={{
                        border: "1px solid #CBD5E0",
                        borderRadius: "8px",
                        outline: "none",
                        padding: "2px",
                      }}
                      theme={colorMode === "dark" ? githubDark : githubLight}
                      width="765px"
                    />
                    {Boolean(errors.conf) ? (
                      <Field.ErrorText>{errors.conf}</Field.ErrorText>
                    ) : undefined}
                  </Field.Root>
                )}
              />

              <Controller
                control={control}
                name="note"
                render={({ field }) => (
                  <Field.Root mt={6}>
                    <Field.Label fontSize="md">Dag Run Notes</Field.Label>
                    <Input {...field} placeholder="Optional" size="sm" />
                  </Field.Root>
                )}
              />
            </Box>
          </Accordion.ItemContent>
        </Accordion.Item>
      </Accordion.Root>
      <ErrorAlert error={errorTrigger} />
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          {isDirty ? (
            <Button onClick={handleReset} variant="outline">
              Reset
            </Button>
          ) : undefined}
          <Spacer />
          <Button
            colorPalette="blue"
            disabled={Boolean(errors.conf) || Boolean(errors.date) || isPending}
            onClick={() => void handleSubmit(onSubmit)()}
          >
            <FiPlay /> Trigger
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default TriggerDAGForm;
