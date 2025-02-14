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
import { Controller, useForm } from "react-hook-form";
import { FiSave } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { Button } from "src/components/ui";
import { Checkbox } from "src/components/ui/Checkbox";

export type PoolBody = {
  description: string | undefined;
  include_deferred: boolean;
  name: string;
  slots: number;
};

type PoolFormProps = {
  readonly error: unknown;
  readonly initialPool: PoolBody;
  readonly isPending: boolean;
  readonly manageMutate: (poolRequestBody: PoolBody) => void;
  readonly setError: (error: unknown) => void;
};

const PoolForm = ({ error, initialPool, isPending, manageMutate, setError }: PoolFormProps) => {
  const {
    control,
    formState: { isDirty, isValid },
    handleSubmit,
    reset,
  } = useForm<PoolBody>({
    defaultValues: initialPool,
    mode: "onChange",
  });

  const onSubmit = (data: PoolBody) => {
    manageMutate(data);
  };

  const handleReset = () => {
    setError(undefined);
    reset();
  };

  return (
    <>
      <Controller
        control={control}
        name="name"
        render={({ field, fieldState }) => (
          <Field.Root invalid={Boolean(fieldState.error)} required>
            <Field.Label fontSize="md">
              Name <Field.RequiredIndicator />
            </Field.Label>
            <Input {...field} disabled={Boolean(initialPool.name)} required size="sm" />
            {fieldState.error ? <Field.ErrorText>{fieldState.error.message}</Field.ErrorText> : undefined}
          </Field.Root>
        )}
        rules={{
          required: "Name is required",
          validate: (_value) => _value.length <= 256 || "Name can contain a maximum of 256 characters",
        }}
      />

      <Controller
        control={control}
        name="slots"
        render={({ field }) => (
          <Field.Root mt={4}>
            <Field.Label fontSize="md">Slots</Field.Label>
            <Input {...field} min={initialPool.slots} size="sm" type="number" />
          </Field.Root>
        )}
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

      <Controller
        control={control}
        name="include_deferred"
        render={({ field }) => (
          <Field.Root mb={4} mt={4}>
            <Field.Label fontSize="md">Include Deferred</Field.Label>
            <Checkbox checked={field.value} colorPalette="blue" onChange={field.onChange} size="sm">
              Check to include deferred tasks when calculating open pool slots
            </Checkbox>
          </Field.Root>
        )}
      />

      <ErrorAlert error={error} />

      <Box as="footer" display="flex" justifyContent="flex-end" mt={8}>
        <HStack w="full">
          {isDirty ? (
            <Button onClick={handleReset} variant="outline">
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

export default PoolForm;
