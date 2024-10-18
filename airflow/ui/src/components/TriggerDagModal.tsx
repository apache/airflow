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
  Box,
  Button,
  useDisclosure,
  Text,
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionIcon,
  AccordionPanel,
  FormControl,
  FormLabel,
  Input,
  FormErrorMessage,
  Tr,
  Td,
  Table,
} from "@chakra-ui/react";
import { json, jsonParseLinter } from "@codemirror/lang-json";
import { linter, lintGutter } from "@codemirror/lint";
import { indentationMarkers } from "@replit/codemirror-indentation-markers";
import CodeMirror, {
  lineNumbers,
  type ViewUpdate,
} from "@uiw/react-codemirror";
import { useForm, SubmitHandler, Controller } from "react-hook-form";
import { FiPlay } from "react-icons/fi";

import ActionModal from "./ActionModal";

type Props = {
  readonly dagDisplayName: string;
  readonly dagId: string;
};

type TriggerDagModalProps = {
  readonly isOpen: boolean;
  readonly onClose: () => void;
} & Props;

type FormValues = {
  conf: string;
  logical_date: string;
  run_id: string;
};

export const TriggerDagModal = ({
  dagDisplayName,
  dagId,
  isOpen,
  onClose,
}: TriggerDagModalProps) => {
  const onTrigger = (values: FormValues) =>
    new Promise<void>((resolve) => {
      setTimeout(() => {
        // TODO of course this alert must be removed and replaced with the final DAG trigger via API
        alert(JSON.stringify(values, null, 2));
        resolve();
        // Trigger here TODO
        onClose();
      }, 2000);
    });

  const {
    control,
    formState: { errors, isSubmitting },
    handleSubmit,
    register,
  } = useForm<FormValues>();

  return (
    <ActionModal
      header={`Trigger DAG ${dagDisplayName}`}
      isOpen={isOpen}
      onClose={onClose}
      submitButton={
        <Button isLoading={isSubmitting} onClick={handleSubmit(onTrigger)}>
          Trigger
        </Button>
      }
    >
      <Box>
        <Accordion allowToggle>
          <AccordionItem>
            <AccordionButton>
              <Box flex="1" textAlign="left">
                <Text as="strong" size="lg">
                  DAG Run Options
                </Text>
              </Box>
              <AccordionIcon />
            </AccordionButton>
            <AccordionPanel>
              <form onSubmit={handleSubmit(onTrigger)}>
                <FormControl isInvalid={Boolean(errors.root?.message)}>
                  <Table colorScheme="blue">
                    <Tr>
                      <Td>
                        <FormLabel htmlFor="logical_date">
                          Logical date:
                        </FormLabel>
                      </Td>
                      <Td>
                        <Input
                          id="logical_date"
                          type="date"
                          {...register("logical_date")}
                        />
                        <FormErrorMessage>
                          {errors.root?.message}
                        </FormErrorMessage>
                      </Td>
                    </Tr>
                    <Tr>
                      <Td>
                        <FormLabel htmlFor="run_id">Run id:</FormLabel>
                      </Td>
                      <Td>
                        <Input
                          id="run_id"
                          placeholder="Run id, optional - will be generated if not provided"
                          {...register("run_id")}
                        />
                        <FormErrorMessage>
                          {errors.root?.message}
                        </FormErrorMessage>
                      </Td>
                    </Tr>
                    <Tr>
                      <Td colSpan={2}>
                        <FormLabel htmlFor="conf">
                          Configuration JSON:
                        </FormLabel>
                        <Controller
                          control={control}
                          defaultValue="{}"
                          name="conf"
                          render={({ field }) => (
                            <CodeMirror
                              {...field}
                              extensions={[
                                lineNumbers(),
                                lintGutter(),
                                json(),
                                indentationMarkers(),
                                linter(jsonParseLinter()),
                              ]}
                              height="200px"
                              id="conf"
                              width="100%"
                              {...register("conf")}
                              onChange={(
                                value: string,
                                viewUpdate: ViewUpdate,
                              ) => {
                                field.onChange(value);
                              }}
                            />
                          )}
                        />
                        <FormErrorMessage>
                          {errors.root?.message}
                        </FormErrorMessage>
                      </Td>
                    </Tr>
                  </Table>
                </FormControl>
              </form>
            </AccordionPanel>
          </AccordionItem>
        </Accordion>
      </Box>
      {/* TODO Trigger form from component */}
      {/* TODO Toggle to unpause DAG if paused */}
      {/* TODO Reset form on open */}
      {/* TODO Form validation needs to block submit */}
    </ActionModal>
  );
};
