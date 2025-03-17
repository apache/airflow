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
import { Input, Button, Box, Spacer, HStack, Field, Stack, VStack, Textarea } from "@chakra-ui/react";
import { Select } from "chakra-react-select";
import { useEffect, useState } from "react";
import { useForm, Controller } from "react-hook-form";
import { FiEye, FiEyeOff, FiSave } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { FlexibleForm, flexibleFormExtraFieldSection } from "src/components/FlexibleForm";
import { JsonEditor } from "src/components/JsonEditor";
import { Accordion } from "src/components/ui";
import { useAddConnection } from "src/queries/useAddConnection";
import { useConnectionTypeMeta } from "src/queries/useConnectionTypeMeta";
import type { ParamsSpec } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";

type AddConnectionFormProps = {
  readonly onClose: () => void;
};

export type AddConnectionParams = {
  conf: string;
  conn_type: string;
  connection_id: string;
  description: string;
  host: string;
  login: string;
  password: string;
  port: string;
  schema: string;
};

const ConnectionForm = ({ onClose }: AddConnectionFormProps) => {
  const [errors, setErrors] = useState<{ conf?: string }>({});
  const { formattedData: connectionTypeMeta, keysList: connectionTypes } = useConnectionTypeMeta();
  const { addConnection, error, isPending } = useAddConnection({ onSuccessConfirm: onClose });
  const { conf, setConf } = useParamStore();
  const [showPassword, setShowPassword] = useState(false);

  const {
    control,
    formState: { isValid },
    handleSubmit,
    reset,
    watch,
  } = useForm<AddConnectionParams>({
    defaultValues: {
      conf,
      conn_type: "",
      connection_id: "",
      description: "",
      host: "",
      login: "",
      password: "",
      port: "",
      schema: "",
    },
  });

  const selectedConnType = watch("conn_type"); // Get the selected connection type
  const standardFields = connectionTypeMeta[selectedConnType]?.standard_fields ?? {};
  const paramsDic = { paramsDict: connectionTypeMeta[selectedConnType]?.extra_fields ?? ({} as ParamsSpec) };

  useEffect(() => {
    reset((prevValues) => ({
      ...prevValues,
      conn_type: selectedConnType,
      description: "",
      host: "",
      login: "",
      password: "",
      port: "",
      schema: "",
    }));
  }, [selectedConnType, reset]);

  // Automatically reset form when conf is fetched
  useEffect(() => {
    reset((prevValues) => ({
      ...prevValues, // Retain existing form values
      conf,
    }));
  }, [conf, reset, setConf]);

  const onSubmit = (data: AddConnectionParams) => {
    addConnection(data);
  };

  const validateAndPrettifyJson = (value: string) => {
    try {
      const parsedJson = JSON.parse(value) as JSON;

      setErrors((prev) => ({ ...prev, conf: undefined }));

      const formattedJson = JSON.stringify(parsedJson, undefined, 2);

      if (formattedJson !== conf) {
        setConf(formattedJson); // Update only if the value is different
      }

      return formattedJson;
    } catch (error_) {
      const errorMessage = error_ instanceof Error ? error_.message : "Unknown error occurred.";

      setErrors((prev) => ({
        ...prev,
        conf: `Invalid JSON format: ${errorMessage}`,
      }));

      return value;
    }
  };

  const connTypesOptions = connectionTypes.map((conn) => ({
    label: conn,
    value: conn,
  }));

  return (
    <>
      <VStack gap={5} p={3}>
        <Controller
          control={control}
          name="connection_id"
          render={({ field, fieldState }) => (
            <Field.Root invalid={Boolean(fieldState.error)} orientation="horizontal" required>
              <Stack>
                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                  Connection ID <Field.RequiredIndicator />
                </Field.Label>
              </Stack>
              <Stack css={{ flexBasis: "70%" }}>
                <Input {...field} required size="sm" />
                {fieldState.error ? <Field.ErrorText>{fieldState.error.message}</Field.ErrorText> : undefined}
              </Stack>
            </Field.Root>
          )}
          rules={{
            required: "Connection ID is required",
          }}
        />

        <Controller
          control={control}
          name="conn_type"
          render={({ field: { onChange, value } }) => (
            <Field.Root orientation="horizontal" required>
              <Stack>
                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                  Connection Type <Field.RequiredIndicator />
                </Field.Label>
              </Stack>
              <Stack css={{ flexBasis: "70%" }}>
                <Select
                  {...Field}
                  onChange={(val) => onChange(val?.value)}
                  options={connTypesOptions}
                  placeholder="Select Connection Type"
                  value={connTypesOptions.find((type) => type.value === value)}
                />
                <Field.HelperText>
                  Connection type missing? Make sure you have installed the corresponding Airflow Providers
                  Package.
                </Field.HelperText>
              </Stack>
            </Field.Root>
          )}
        />

        {selectedConnType ? (
          <>
            <Accordion.Root collapsible mb={4} mt={4} size="lg" variant="enclosed">
              <Accordion.Item key="standardField" value="standardField">
                <Accordion.ItemTrigger cursor="button">Standard Fields</Accordion.ItemTrigger>
                <Accordion.ItemContent>
                  <Stack pb={3} pl={3} pr={3}>
                    {Object.entries(standardFields).map(([key, fields]) => {
                      if (Boolean(fields.hidden)) {
                        return undefined;
                      } // Skip hidden fields

                      return (
                        <Controller
                          control={control}
                          key={key}
                          name={key as keyof AddConnectionParams}
                          render={({ field }) => (
                            <Field.Root mt={3} orientation="horizontal">
                              <Stack>
                                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                                  {fields.title ?? key}
                                </Field.Label>
                              </Stack>
                              <Stack css={{ flexBasis: "70%", position: "relative" }}>
                                {key === "description" ? (
                                  <Textarea {...field} placeholder={fields.placeholder ?? ""} />
                                ) : (
                                  <div style={{ position: "relative", width: "100%" }}>
                                    <Input
                                      {...field}
                                      placeholder={fields.placeholder ?? ""}
                                      type={key === "password" && !showPassword ? "password" : "text"}
                                    />
                                    {key === "password" && (
                                      <button
                                        onClick={() => setShowPassword(!showPassword)}
                                        style={{
                                          cursor: "pointer",
                                          position: "absolute",
                                          right: "10px",
                                          top: "50%",
                                          transform: "translateY(-50%)",
                                        }}
                                        type="button"
                                      >
                                        {showPassword ? <FiEye size={15} /> : <FiEyeOff size={15} />}
                                      </button>
                                    )}
                                  </div>
                                )}
                              </Stack>
                            </Field.Root>
                          )}
                        />
                      );
                    })}
                  </Stack>
                </Accordion.ItemContent>
              </Accordion.Item>
              <FlexibleForm
                flexibleFormDefaultSection={flexibleFormExtraFieldSection}
                initialParamsDict={paramsDic}
                key={selectedConnType}
              />
            </Accordion.Root>

            <Controller
              control={control}
              name="conf"
              render={({ field }) => (
                <Field.Root invalid={Boolean(errors.conf)}>
                  <Field.Label fontSize="md" mb={3}>
                    Extra Fields JSON
                  </Field.Label>
                  <JsonEditor
                    {...field}
                    onBlur={() => {
                      field.onChange(validateAndPrettifyJson(field.value));
                    }}
                  />
                  {Boolean(errors.conf) ? <Field.ErrorText>{errors.conf}</Field.ErrorText> : undefined}
                </Field.Root>
              )}
            />
          </>
        ) : undefined}
      </VStack>

      <ErrorAlert error={error} />
      <Box as="footer" display="flex" justifyContent="flex-end" mr={3} mt={4}>
        <HStack w="full">
          <Spacer />
          <Button
            colorPalette="blue"
            disabled={Boolean(errors.conf) || isPending || !isValid}
            onClick={() => void handleSubmit(onSubmit)()}
          >
            <FiSave /> Save
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default ConnectionForm;
