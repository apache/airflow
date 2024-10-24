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
    FormControl,
    FormLabel,
    Input,
    VStack,
    ModalCloseButton,
    Button,
    ModalFooter,
    Box,
    Text,
    Spacer,
    HStack,
    Collapse,
  } from "@chakra-ui/react";
  import React, { useState, useEffect } from "react";
  import CodeMirror from "@uiw/react-codemirror";
  import { json } from "@codemirror/lang-json";
  import { autocompletion } from "@codemirror/autocomplete";
  import { oneDark } from "@codemirror/theme-one-dark";
  
  type DagParams = {
    configJson: string;
    dagId: string;
    logicalDate: string;
    runId?: string;
  };
  
  type TriggerDAGFormProps = {
    dagParams: DagParams;
    onClose: () => void;
    onTrigger: () => void;
    setDagParams: React.Dispatch<React.SetStateAction<DagParams>>;
  };
  
  const TriggerDAGForm: React.FC<TriggerDAGFormProps> = ({
    dagParams,
    onClose,
    onTrigger,
    setDagParams,
  }) => {
    const [showDetails, setShowDetails] = useState(false); // State to show/hide all details
  
    // Automatically format JSON whenever the configJson changes
    useEffect(() => {
      try {
        const prettyJson = JSON.stringify(
          JSON.parse(dagParams.configJson),
          null,
          2
        );
        setDagParams((prev) => ({ ...prev, configJson: prettyJson }));
      } catch {
        // Invalid JSON handling can go here (e.g., highlight or notification)
      }
    }, [dagParams.configJson, setDagParams]);
  
    const handleChange = (
      ele: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
    ) => {
      const { name, value } = ele.target;
      setDagParams((prev) => ({ ...prev, [name]: value }));
    };
  
    const handleJsonChange = (value: string) => {
      setDagParams((prev) => ({ ...prev, configJson: value }));
    };
  
    const isValidJson = () => {
      try {
        JSON.parse(dagParams.configJson);
        return true;
      } catch {
        return false;
      }
    };
  
    return (
      <>
        <ModalCloseButton />
  
        {/* Toggle show/hide for all details */}
        <VStack align="stretch" p={4} spacing={4}>
          <Button
            variant="outline"
            onClick={() => setShowDetails(!showDetails)}
            width="full"
          >
            {showDetails ? "Hide Details" : "Show Details"}
          </Button>
  
          <Collapse in={showDetails}>
            <VStack align="stretch" spacing={4}>
              {/* Logical date/time input with timezone */}
              <FormControl>
                <FormLabel>Logical date (with time)</FormLabel>
                <Input
                  boxShadow="md"
                  name="logicalDate"
                  onChange={handleChange}
                  placeholder="yyyy-mm-ddThh:mm"
                  type="datetime-local" // Allows date and time selection
                  value={dagParams.logicalDate}
                />
              </FormControl>
  
              {/* Run ID input */}
              <FormControl>
                <FormLabel>Run ID (Optional)</FormLabel>
                <Input
                  boxShadow="md"
                  name="runId"
                  onChange={handleChange}
                  placeholder="Run ID (Optional - autogenerated if left empty)"
                  value={dagParams.runId}
                />
              </FormControl>
  
              {/* JSON Configuration with CodeMirror */}
              <FormControl>
                <FormLabel>Configuration JSON</FormLabel>
                <Box border="1px" borderColor="gray.300" borderRadius="md">
                  <CodeMirror
                    value={dagParams.configJson}
                    height="200px"
                    extensions={[json(), autocompletion()]}
                    theme={oneDark}
                    onChange={handleJsonChange}
                  />
                </Box>
                {!isValidJson() ? (
                  <Box color="red.500" mt={2}>
                    <Text fontSize="sm">Invalid JSON format</Text>
                  </Box>
                ) : null}
              </FormControl>
            </VStack>
          </Collapse>
        </VStack>
  
        <ModalFooter>
          <HStack w="full">
            {/* Cancel button in red */}
            <Button colorScheme="red" onClick={onClose}>
              Cancel
            </Button>
            <Spacer />
            {/* Trigger button in green */}
            <Button
              colorScheme="green"
              isDisabled={!isValidJson()} // Disable if JSON is invalid
              onClick={onTrigger}
            >
              Trigger
            </Button>
          </HStack>
        </ModalFooter>
      </>
    );
  };
  
  export default TriggerDAGForm;  