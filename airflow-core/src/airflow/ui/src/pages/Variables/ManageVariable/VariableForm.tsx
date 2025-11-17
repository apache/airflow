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
import { Box, Button, Field, HStack, IconButton, Input, Spacer, Text, Textarea } from "@chakra-ui/react";
import { useEffect, useRef, useState } from "react";
import { type ControllerFieldState, type ControllerRenderProps, Controller, useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { FiCode, FiSave } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { TeamSelector } from "src/components/TeamSelector.tsx";
import { useConfig } from "src/queries/useConfig.tsx";
import { isJsonString, minifyJson, prettifyJson } from "src/utils";

type ValueFieldProps = {
  readonly field: ControllerRenderProps<VariableBody, "value">;
  readonly fieldState: ControllerFieldState;
};

const ValueField = ({ field, fieldState }: ValueFieldProps) => {
  const { t: translate } = useTranslation(["admin"]);
  const [displayValue, setDisplayValue] = useState(field.value);
  const fieldValueRef = useRef(field.value);

  useEffect(() => {
    if (fieldValueRef.current !== field.value) {
      fieldValueRef.current = field.value;
      setDisplayValue(field.value);
    }
  }, [field.value]);

  const showJsonWarning =
    displayValue.startsWith("{") || displayValue.startsWith("[") ? !isJsonString(displayValue) : false;

  const handleChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    const newValue = event.target.value;

    setDisplayValue(newValue);
    field.onChange(newValue);
  };

  const handleBlur = () => {
    field.onBlur();
    setDisplayValue(prettifyJson(displayValue));
  };

  const handleFormat = () => {
    const formatted = prettifyJson(displayValue);

    setDisplayValue(formatted);
    field.onChange(formatted);
  };

  return (
    <Field.Root invalid={Boolean(fieldState.error)} mt={4} required>
      <Field.Label fontSize="md">
        {translate("columns.value")} <Field.RequiredIndicator />
      </Field.Label>
      <Box position="relative" w="full">
        <Textarea
          autoresize
          name={field.name}
          onBlur={handleBlur}
          onChange={handleChange}
          ref={field.ref}
          size="sm"
          value={displayValue}
        />
        {isJsonString(displayValue) ? (
          <IconButton
            aria-label="Format JSON"
            onClick={handleFormat}
            position="absolute"
            right={2}
            size="2xs"
            title="Format JSON"
            top={2}
            variant="ghost"
          >
            <FiCode />
          </IconButton>
        ) : undefined}
      </Box>
      {showJsonWarning ? (
        <Text color="fg.warning" fontSize="xs">
          {translate("variables.form.invalidJson")}
        </Text>
      ) : undefined}
      {fieldState.error ? <Field.ErrorText>{fieldState.error.message}</Field.ErrorText> : undefined}
    </Field.Root>
  );
};

export type VariableBody = {
  description: string | undefined;
  key: string;
  team_name: string;
  value: string;
};

type VariableFormProps = {
  readonly error: unknown;
  readonly initialVariable: VariableBody;
  readonly isPending: boolean;
  readonly manageMutate: (variableRequestBody: VariableBody) => void;
  readonly setError: (error: unknown) => void;
};

const VariableForm = ({ error, initialVariable, isPending, manageMutate, setError }: VariableFormProps) => {
  const { t: translate } = useTranslation(["admin", "common"]);
  const {
    control,
    formState: { isDirty, isValid },
    handleSubmit,
    reset,
  } = useForm<VariableBody>({
    defaultValues: initialVariable,
    mode: "onChange",
  });
  const multiTeamEnabled = Boolean(useConfig("multi_team"));

  const onSubmit = (data: VariableBody) => {
    // Minify JSON before submitting if it's valid JSON
    const value = data.value ? minifyJson(data.value) : data.value;

    manageMutate({ ...data, value });
  };

  const handleReset = () => {
    setError(undefined);
    reset();
  };

  return (
    <>
      <Controller
        control={control}
        name="key"
        render={({ field, fieldState }) => (
          <Field.Root invalid={Boolean(fieldState.error)} required>
            <Field.Label fontSize="md">
              {translate("columns.key")} <Field.RequiredIndicator />
            </Field.Label>
            <Input {...field} disabled={Boolean(initialVariable.key)} required size="sm" />
            {fieldState.error ? <Field.ErrorText>{fieldState.error.message}</Field.ErrorText> : undefined}
          </Field.Root>
        )}
        rules={{
          required: translate("variables.form.keyRequired"),
          validate: (_value) => _value.length <= 250 || translate("variables.form.keyMaxLength"),
        }}
      />

      <Controller
        control={control}
        name="value"
        render={({ field, fieldState }) => <ValueField field={field} fieldState={fieldState} />}
        rules={{
          required: translate("variables.form.valueRequired"),
        }}
      />

      <Controller
        control={control}
        name="description"
        render={({ field }) => (
          <Field.Root mb={4} mt={4}>
            <Field.Label fontSize="md">{translate("columns.description")}</Field.Label>
            <Textarea {...field} size="sm" />
          </Field.Root>
        )}
      />

      {multiTeamEnabled ? <TeamSelector control={control} /> : undefined}

      <ErrorAlert error={error} />

      <Box as="footer" display="flex" justifyContent="flex-end" mt={8}>
        <HStack w="full">
          {isDirty ? (
            <Button onClick={handleReset} variant="outline">
              {translate("common:reset")}
            </Button>
          ) : undefined}
          <Spacer />
          <Button
            colorPalette="brand"
            disabled={!isValid || isPending}
            onClick={() => void handleSubmit(onSubmit)()}
          >
            <FiSave /> {translate("formActions.save")}
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default VariableForm;
