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
import { Input, Button, Box, Text, Spacer, HStack } from "@chakra-ui/react";
import { json } from "@codemirror/lang-json";
import { githubLight, githubDark } from "@uiw/codemirror-themes-all";
import CodeMirror from "@uiw/react-codemirror";
import { useEffect, useState } from "react";
import { useForm, Controller } from "react-hook-form";
import { FiPlay } from "react-icons/fi";

import { useColorMode } from "src/context/colorMode";

import { Accordion } from "../ui";
import type { DagParams } from "./TriggerDag";

type TriggerDAGFormProps = {
  dagParams: DagParams;
  onClose: () => void;
  onTrigger: (updatedDagParams: DagParams) => void;
  setDagParams: React.Dispatch<React.SetStateAction<DagParams>>;
};

const TriggerDAGForm: React.FC<TriggerDAGFormProps> = ({
  dagParams,
  onTrigger,
  setDagParams,
}) => {
  const [jsonError, setJsonError] = useState<string | undefined>();

  const {
    control,
    formState: { isDirty },
    handleSubmit,
    reset,
    setValue,
    watch,
  } = useForm<DagParams>({
    defaultValues: dagParams,
  });

  // Watch the date fields for dynamic min/max constraints
  const dataIntervalStart = watch("dataIntervalStart");
  const dataIntervalEnd = watch("dataIntervalEnd");

  useEffect(() => {
    reset(dagParams);
  }, [dagParams, reset]);

  const onSubmit = (data: DagParams) => {
    onTrigger(data);
    setDagParams(data);
    setJsonError(undefined);
  };

  const validateAndPrettifyJson = (value: string) => {
    try {
      const parsedJson = JSON.parse(value) as JSON;

      setJsonError(undefined);

      return JSON.stringify(parsedJson, undefined, 2);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error occurred.";

      setJsonError(`Invalid JSON format: ${errorMessage}`);

      return value;
    }
  };

  // Function to validate and enforce date constraints based on which field is edited
  const validateDates = (
    fieldName: "dataIntervalEnd" | "dataIntervalStart",
  ) => {
    const startDate = dataIntervalStart ? new Date(dataIntervalStart) : null;
    const endDate = dataIntervalEnd ? new Date(dataIntervalEnd) : null;

    if (startDate && endDate) {
      if (fieldName === "dataIntervalStart" && startDate > endDate) {
        setValue("dataIntervalStart", dataIntervalEnd); // Adjust start to match end if invalid
      } else if (fieldName === "dataIntervalEnd" && endDate < startDate) {
        setValue("dataIntervalEnd", dataIntervalStart); // Adjust end to match start if invalid
      }
    }
  };

  const { colorMode } = useColorMode();

  return (
    <>
      <Accordion.Root collapsible size="lg" variant="enclosed">
        <Accordion.Item key="advancedOptions" value="advancedOptions">
          <Accordion.ItemTrigger>Advanced Options</Accordion.ItemTrigger>
          <Accordion.ItemContent>
            <Box p={5}>
              <Text fontSize="md" mb={2}>
                Data Interval Start Date
              </Text>
              <Controller
                control={control}
                name="dataIntervalStart"
                render={({ field }) => (
                  <Input
                    {...field}
                    max={dataIntervalEnd || undefined}
                    onBlur={() => validateDates("dataIntervalStart")}
                    placeholder="yyyy-mm-ddThh:mm"
                    size="sm"
                    type="datetime-local"
                  />
                )}
              />

              <Text fontSize="md" mb={2} mt={6}>
                Data Interval End Date
              </Text>
              <Controller
                control={control}
                name="dataIntervalEnd"
                render={({ field }) => (
                  <Input
                    {...field}
                    min={dataIntervalStart || undefined}
                    onBlur={() => validateDates("dataIntervalEnd")}
                    placeholder="yyyy-mm-ddThh:mm"
                    size="sm"
                    type="datetime-local"
                  />
                )}
              />

              <Text fontSize="md" mb={2} mt={6}>
                Run ID
              </Text>
              <Controller
                control={control}
                name="runId"
                render={({ field }) => (
                  <Input
                    {...field}
                    placeholder="Run id, optional - will be generated if not provided"
                    size="sm"
                  />
                )}
              />

              <Text fontSize="md" mb={2} mt={6}>
                Configuration JSON
              </Text>
              <Controller
                control={control}
                name="configJson"
                render={({ field }) => (
                  <Box mb={4}>
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
                    />
                    {Boolean(jsonError) ? (
                      <Text color="red.500" fontSize="sm" mt={2}>
                        {jsonError}
                      </Text>
                    ) : undefined}
                  </Box>
                )}
              />
            </Box>
          </Accordion.ItemContent>
        </Accordion.Item>
      </Accordion.Root>

      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          {isDirty ? (
            <Button onClick={() => reset()} variant="outline">
              Reset
            </Button>
          ) : undefined}
          <Spacer />
          <Button
            colorPalette="blue"
            disabled={Boolean(jsonError)}
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
