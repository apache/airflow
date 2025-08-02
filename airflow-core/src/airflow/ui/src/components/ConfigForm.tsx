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
import { Accordion, Box, Field } from "@chakra-ui/react";
import { type Control, type FieldValues, type Path, Controller } from "react-hook-form";

import type { ParamsSpec } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";

import { FlexibleForm, flexibleFormDefaultSection } from "./FlexibleForm";
import { JsonEditor } from "./JsonEditor";

type ConfigFormProps<T extends FieldValues = FieldValues> = {
  readonly children?: React.ReactNode;
  readonly control: Control<T>;
  readonly errors: {
    conf?: string;
    date?: unknown;
  };
  readonly initialParamsDict: { paramsDict: ParamsSpec };
  readonly setErrors: React.Dispatch<
    React.SetStateAction<{
      conf?: string;
      date?: unknown;
    }>
  >;
  readonly setFormError: (error: boolean) => void;
};

const ConfigForm = <T extends FieldValues = FieldValues>({
  children,
  control,
  errors,
  initialParamsDict,
  setErrors,
  setFormError,
}: ConfigFormProps<T>) => {
  const { conf, setConf } = useParamStore();

  const validateAndPrettifyJson = (value: string) => {
    try {
      const parsedJson = JSON.parse(value) as JSON;

      setErrors((prev) => ({ ...prev, conf: undefined }));

      const formattedJson = JSON.stringify(parsedJson, undefined, 2);

      if (formattedJson !== conf) {
        setConf(formattedJson); // Update only if the value is different
      }

      return formattedJson;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error occurred.";

      setErrors((prev) => ({
        ...prev,
        conf: `Invalid JSON format: ${errorMessage}`,
      }));

      return value;
    }
  };

  return (
    <Accordion.Root
      collapsible
      defaultValue={[flexibleFormDefaultSection]}
      mb={4}
      overflow="visible"
      size="lg"
      variant="enclosed"
    >
      <FlexibleForm
        flexibleFormDefaultSection={flexibleFormDefaultSection}
        initialParamsDict={initialParamsDict}
        setError={setFormError}
      />
      <Accordion.Item key="advancedOptions" value="advancedOptions">
        <Accordion.ItemTrigger cursor="button">Advanced Options</Accordion.ItemTrigger>
        <Accordion.ItemContent>
          <Box p={4}>
            {children}
            <Controller
              control={control}
              name={"conf" as Path<T>}
              render={({ field }) => (
                <Field.Root invalid={Boolean(errors.conf)} mt={6}>
                  <Field.Label fontSize="md">Configuration JSON</Field.Label>
                  <JsonEditor
                    {...field}
                    onBlur={() => {
                      field.onChange(validateAndPrettifyJson(field.value as string));
                    }}
                  />
                  {Boolean(errors.conf) ? <Field.ErrorText>{errors.conf}</Field.ErrorText> : undefined}
                </Field.Root>
              )}
            />
          </Box>
        </Accordion.ItemContent>
      </Accordion.Item>
    </Accordion.Root>
  );
};

export default ConfigForm;
