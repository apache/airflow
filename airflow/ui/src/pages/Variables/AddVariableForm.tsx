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
import { Box, HStack, Input, Spacer, Text } from "@chakra-ui/react";
import { useEffect, useMemo } from "react";
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
  onClose: () => void;
};

const AddVariableForm: React.FC<AddVariableFormProps> = ({ onClose }) => {
  const { addVariable, error: errorAdd, isPending } = useAddVariable(onClose);
  const addVariableParams: AddVariableBody = useMemo(
    () => ({
      description: undefined,
      key: "",
      value: "",
    }),
    [],
  );

  const {
    control,
    formState: { isDirty },
    handleSubmit,
    reset,
    watch,
  } = useForm<AddVariableBody>({
    defaultValues: addVariableParams,
  });

  const { key, value } = watch();

  useEffect(() => {
    reset(addVariableParams);
  }, [addVariableParams, reset]);

  const onSubmit = (data: AddVariableBody) => {
    addVariable(data);
  };

  return (
    <>
      <ErrorAlert error={errorAdd} />
      <Text fontSize="md" mb={2} mt={4}>
        Key <span style={{ color: "red" }}>*</span>
      </Text>
      <Controller
        control={control}
        name="key"
        render={({ field }) => <Input {...field} required size="sm" />}
      />

      <Text fontSize="md" mb={2} mt={4}>
        Value <span style={{ color: "red" }}>*</span>
      </Text>
      <Controller
        control={control}
        name="value"
        render={({ field }) => <Input {...field} required size="sm" />}
      />

      <Text fontSize="md" mb={2} mt={4}>
        Description
      </Text>
      <Controller
        control={control}
        name="description"
        render={({ field }) => <Input {...field} required size="sm" />}
      />
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
            disabled={key === "" || value === "" || Boolean(isPending)}
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
