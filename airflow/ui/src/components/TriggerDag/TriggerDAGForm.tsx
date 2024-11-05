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
import {
  Input,
  VStack,
  Dialog,
  Button,
  Box,
  Text,
  Spacer,
  HStack,
} from "@chakra-ui/react";
import { autocompletion } from "@codemirror/autocomplete";
import { json } from "@codemirror/lang-json";
import { githubLight, githubDark } from "@uiw/codemirror-themes-all";
import CodeMirror, { type Extension, lineNumbers } from "@uiw/react-codemirror";
import { useEffect, useState } from "react";
import { useForm, Controller } from "react-hook-form";

import { useColorMode } from "src/context/colorMode";

import type { DagParams } from "./TriggerDag";

type TriggerDAGFormProps = {
  dagParams: DagParams;
  onClose: () => void;
  onTrigger: () => void;
  setDagParams: React.Dispatch<React.SetStateAction<DagParams>>;
};

const TriggerDAGForm: React.FC<TriggerDAGFormProps> = ({
  dagParams,
  onTrigger,
}) => {
  const [showDetails, setShowDetails] = useState(false);
  const [jsonError, setJsonError] = useState<string | undefined>(undefined); // Track JSON error
  const { control, handleSubmit, reset, watch } = useForm({
    defaultValues: {
      configJson: JSON.stringify(dagParams.configJson), // Ensure it's a string in the form control
      logicalDate: dagParams.logicalDate,
      runId: dagParams.runId,
    },
  });

  useEffect(() => {
    reset({
      configJson: JSON.stringify(dagParams.configJson),
      logicalDate: dagParams.logicalDate,
      runId: dagParams.runId,
    });
  }, [dagParams, reset]);

  const onSubmit = () => {
    onTrigger();
  };

  const hasFormChanged = () => {
    const currentValues = {
      configJson: watch("configJson"),
      logicalDate: watch("logicalDate"),
      runId: watch("runId"),
    };

    return (
      currentValues.configJson !== JSON.stringify(dagParams.configJson) ||
      currentValues.logicalDate !== dagParams.logicalDate ||
      currentValues.runId !== dagParams.runId
    );
  };

  const { colorMode } = useColorMode();

  return (
    <>
      <Dialog.CloseTrigger />

      <VStack align="stretch" gap={2}>
        <Button
          mb={9}
          onClick={() => setShowDetails(!showDetails)}
          variant="outline"
          width="full"
        >
          {showDetails ? "Hide Advanced Options" : "Show Advanced Options"}
        </Button>

        {showDetails ? (
          <VStack align="stretch" gap={3}>
            <Box>
              <Text fontSize="md" mb={2}>
                Logical date
              </Text>
              <Controller
                control={control}
                name="logicalDate"
                render={({ field }) => (
                  <Input
                    {...field}
                    placeholder="yyyy-mm-ddThh:mm"
                    size="sm"
                    type="datetime-local"
                  />
                )}
              />
            </Box>

            <Box>
              <Text fontSize="md" mb={2}>
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
            </Box>

            <Box mb={9}>
              <Text fontSize="md" mb={2}>
                Configuration JSON
              </Text>
              <Controller
                control={control}
                name="configJson"
                render={({ field }) => (
                  <Box>
                    <CodeMirror
                      {...field}
                      basicSetup
                      extensions={[json(), autocompletion(), lineNumbers()]}
                      height="200px"
                      onChange={(value) => {
                        field.onChange(value); // Update the form state
                        try {
                          // Attempt to parse the value as JSON
                          JSON.parse(value);
                          setJsonError(undefined); // Clear error if JSON is valid
                        } catch {
                          setJsonError("Invalid JSON format."); // Set error message if invalid
                        }
                      }}
                      style={{
                        border: "1px solid #CBD5E0",
                        borderRadius: "8px",
                        outline: "none",
                        padding: "2px",
                      }}
                      theme={
                        colorMode === "dark"
                          ? (githubDark as Extension)
                          : (githubLight as Extension)
                      }
                    />
                    {jsonError! ? (
                      <Box color="red.500" mt={2}>
                        <Text fontSize="sm">{jsonError}</Text>
                      </Box>
                    ) : undefined}
                  </Box>
                )}
              />
            </Box>
          </VStack>
        ) : undefined}
      </VStack>

      <Box as="footer" display="flex" justifyContent="flex-end">
        <HStack w="full">
          {hasFormChanged() && <Button onClick={() => reset()}>Reset</Button>}
          <Spacer />
          <Button
            disabled={Boolean(jsonError)} // Disable if there's an error
            onClick={() => void handleSubmit(onSubmit)()}
          >
            Trigger
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default TriggerDAGForm;
