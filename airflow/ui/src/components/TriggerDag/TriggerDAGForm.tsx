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
import { autocompletion } from "@codemirror/autocomplete";
import { json } from "@codemirror/lang-json";
import { githubLight, githubDark } from "@uiw/codemirror-themes-all";
import CodeMirror, { lineNumbers } from "@uiw/react-codemirror";
import { useEffect, useState } from "react";
import { useForm, Controller } from "react-hook-form";

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
  const [jsonError, setJsonError] = useState<string | undefined>(undefined);
  const { control, formState: { isDirty }, handleSubmit, reset } = useForm<DagParams>({
    defaultValues: {
      configJson: dagParams.configJson,
      logicalDate: dagParams.logicalDate,
      runId: dagParams.runId,
    }
});

  useEffect(() => {
    reset({
      configJson: dagParams.configJson, 
      logicalDate: dagParams.logicalDate,
      runId: dagParams.runId,
    });
  }, [dagParams, reset]);

  const onSubmit = (data: DagParams) => {
      onTrigger(data);
      setDagParams(data); 
      setJsonError(undefined); 
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
                      basicSetup
                      extensions={[json(), autocompletion(), lineNumbers()]}
                      height="200px"
                      onChange={(value) => {
                        try {
                          const parsedConfigJson = JSON.parse(value) as Record<string, unknown>;
                          
                          field.onChange(parsedConfigJson); // Update react-hook-form value
                          setJsonError(undefined);
                        } catch {
                          setJsonError("Invalid JSON format.");
                        }
                      }}
                      style={{
                        border: "1px solid #CBD5E0",
                        borderRadius: "8px",
                        outline: "none",
                        padding: "2px",
                      }}
                      theme={colorMode === "dark" ? githubDark : githubLight}
                      value={JSON.stringify(field.value)}
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
          {isDirty ? <Button onClick={() => reset()}>Reset</Button> : undefined}
          <Spacer />
          <Button
            colorPalette="blue"
            disabled={Boolean(jsonError)}
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
