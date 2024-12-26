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
import { Box, Field, HStack, Input, Spacer, Textarea } from "@chakra-ui/react";
import { useEffect } from "react";
import { Controller, useForm } from "react-hook-form";
import { FiSave } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { Button } from "src/components/ui";
import { useAddVariable } from "src/queries/useAddVariable";

export type AddVariableBody = {
  description: string | undefined;
  key: string;
  value: string;
};

type AddVariableFormProps = {
  readonly onClose: () => void;
};

const AddVariableForm = ({ onClose }: AddVariableFormProps) => {
  const { addVariable, error, isPending } = useAddVariable(onClose);

  const {
    control,
    formState: { isDirty, isValid },
    handleSubmit,
    reset,
  } = useForm<AddVariableBody>({
    defaultValues: {
      description: "",
      key: "",
      value: "",
    },
    mode: "onChange",
  });

  useEffect(() => {
    reset({
      description: "",
      key: "",
      value: "",
    });
  }, [reset]);

  const onSubmit = (data: AddVariableBody) => {
    addVariable(data);
  };

  return (
    <>
      <Controller
        control={control}
        name="key"
        render={({ field, fieldState }) => (
          <Field.Root invalid={Boolean(fieldState.error)} required>
            <Field.Label fontSize="md">
              Key <Field.RequiredIndicator />
            </Field.Label>
            <Input {...field} required size="sm" />
            {fieldState.error ? (
              <Field.ErrorText>{fieldState.error.message}</Field.ErrorText>
            ) : undefined}
          </Field.Root>
        )}
        rules={{
          required: "Key is required",
          validate: (_value) =>
            _value.length <= 250 ||
            "Key can contain a maximum of 250 characters",
        }}
      />

      <Controller
        control={control}
        name="value"
        render={({ field, fieldState }) => (
          <Field.Root invalid={Boolean(fieldState.error)} mt={4} required>
            <Field.Label fontSize="md">
              Value <Field.RequiredIndicator />
            </Field.Label>
            <Textarea {...field} required size="sm" />
            {fieldState.error ? (
              <Field.ErrorText>{fieldState.error.message}</Field.ErrorText>
            ) : undefined}
          </Field.Root>
        )}
        rules={{
          required: "Value is required",
        }}
      />

      <Controller
        control={control}
        name="description"
        render={({ field }) => (
          <Field.Root mb={4} mt={4}>
            <Field.Label fontSize="md">Description</Field.Label>
            <Textarea {...field} size="sm" />
          </Field.Root>
        )}
      />

      <ErrorAlert error={error} />

      <Box as="footer" display="flex" justifyContent="flex-end" mt={8}>
        <HStack w="full">
          {isDirty ? (
            <Button onClick={() => reset()} variant="outline">
              Reset
            </Button>
          ) : undefined}
          <Spacer />
          <Button
            colorPalette="blue"
            disabled={!isValid || isPending}
            onClick={() => void handleSubmit(onSubmit)()}
          >
            <FiSave /> Save
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default AddVariableForm;
