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
import { Input, Button, Box, Spacer, HStack, Field, Stack, VStack, Spinner } from "@chakra-ui/react";
import { Select } from "chakra-react-select";
import { useEffect, useState } from "react";
import { useForm, Controller } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { FiSave } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { FlexibleForm } from "src/components/FlexibleForm";
import { JsonEditor } from "src/components/JsonEditor";
import { Accordion } from "src/components/ui";
import { useConnectionTypeMeta } from "src/queries/useConnectionTypeMeta";
import type { ParamsSpec } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";

import StandardFields from "./ConnectionStandardFields";
import type { ConnectionBody } from "./Connections";

type AddConnectionFormProps = {
  readonly error: unknown;
  readonly initialConnection: ConnectionBody;
  readonly isEditMode?: boolean;
  readonly isPending: boolean;
  readonly mutateConnection: (requestBody: ConnectionBody) => void;
};

const ConnectionForm = ({
  error,
  initialConnection,
  isEditMode = false,
  isPending,
  mutateConnection,
}: AddConnectionFormProps) => {
  const [errors, setErrors] = useState<{ conf?: string }>({});
  const {
    formattedData: connectionTypeMeta,
    hookNames: hookNameMap,
    isPending: isMetaPending,
    keysList: connectionTypes,
  } = useConnectionTypeMeta();
  const { conf: extra, setConf } = useParamStore();
  const {
    control,
    formState: { isDirty, isValid },
    handleSubmit,
    reset,
    watch,
  } = useForm<ConnectionBody>({
    defaultValues: initialConnection,
    mode: "onBlur",
  });

  const { t: translate } = useTranslation(["admin", "common"]);
  const selectedConnType = watch("conn_type"); // Get the selected connection type
  const standardFields = connectionTypeMeta[selectedConnType]?.standard_fields ?? {};
  const paramsDic = { paramsDict: connectionTypeMeta[selectedConnType]?.extra_fields ?? ({} as ParamsSpec) };

  const [formErrors, setFormErrors] = useState(false);

  useEffect(() => {
    reset((prevValues) => ({
      ...initialConnection,
      conn_type: selectedConnType,
      connection_id: prevValues.connection_id,
    }));
    setConf(JSON.stringify(JSON.parse(initialConnection.extra), undefined, 2));
  }, [selectedConnType, reset, initialConnection, setConf]);

  // Automatically reset form when conf is fetched
  useEffect(() => {
    reset((prevValues) => ({
      ...prevValues, // Retain existing form values
      extra,
    }));
  }, [extra, reset, setConf]);

  const onSubmit = (data: ConnectionBody) => {
    mutateConnection(data);
  };

  // Check if extra fields have changed by comparing with initial connection
  const isExtraFieldsDirty = (() => {
    try {
      const initialParsed = JSON.parse(initialConnection.extra) as Record<string, unknown>;
      const currentParsed = JSON.parse(extra) as Record<string, unknown>;

      return JSON.stringify(initialParsed) !== JSON.stringify(currentParsed);
    } catch {
      // If parsing fails, fall back to string comparison
      return extra !== initialConnection.extra;
    }
  })();

  const validateAndPrettifyJson = (value: string) => {
    try {
      if (value.trim() === "") {
        setErrors((prev) => ({ ...prev, conf: undefined }));

        return value;
      }
      const parsedJson = JSON.parse(value) as Record<string, unknown>;

      if (typeof parsedJson !== "object" || Array.isArray(parsedJson)) {
        throw new TypeError('extra fields must be a valid JSON object (e.g., {"key": "value"})');
      }

      setErrors((prev) => ({ ...prev, conf: undefined }));
      const formattedJson = JSON.stringify(parsedJson, undefined, 2);

      if (formattedJson !== extra) {
        setConf(formattedJson); // Update only if the value is different
      }

      return formattedJson;
    } catch (error_) {
      const errorMessage = error_ instanceof Error ? error_.message : translate("common:error.unknown");

      setErrors((prev) => ({
        ...prev,
        conf: `Invalid JSON format: ${errorMessage}`,
      }));

      return value;
    }
  };

  const connTypesOptions = connectionTypes.map((conn) => ({
    label: hookNameMap[conn],
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
                  {translate("connections.columns.connectionId")} <Field.RequiredIndicator />
                </Field.Label>
              </Stack>
              <Stack css={{ flexBasis: "70%" }}>
                <Input {...field} disabled={Boolean(initialConnection.connection_id)} required size="sm" />
                {fieldState.error ? <Field.ErrorText>{fieldState.error.message}</Field.ErrorText> : undefined}
              </Stack>
            </Field.Root>
          )}
          rules={{
            required: translate("connections.form.connectionIdRequired"),
            validate: (value) =>
              value.trim() === "" ? translate("connections.form.connectionIdRequirement") : true,
          }}
        />

        <Controller
          control={control}
          name="conn_type"
          render={({ field: { onChange, value }, fieldState }) => (
            <Field.Root invalid={Boolean(fieldState.error)} orientation="horizontal" required>
              <Stack>
                <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                  {translate("connections.columns.connectionType")} <Field.RequiredIndicator />
                </Field.Label>
              </Stack>
              <Stack css={{ flexBasis: "70%" }}>
                <Stack>
                  {isMetaPending ? (
                    <Spinner size="sm" style={{ left: "60%", position: "absolute", top: "20%" }} />
                  ) : undefined}
                  <Select
                    {...Field}
                    isDisabled={isMetaPending}
                    onChange={(val) => onChange(val?.value)}
                    options={connTypesOptions}
                    placeholder={translate("connections.form.selectConnectionType")}
                    value={connTypesOptions.find((type) => type.value === value)}
                  />
                </Stack>
                <Field.HelperText>{translate("connections.form.helperText")}</Field.HelperText>
              </Stack>
            </Field.Root>
          )}
          rules={{
            required: translate("connections.form.connectionTypeRequired"),
          }}
        />

        {selectedConnType ? (
          <Accordion.Root
            collapsible
            defaultValue={["standardFields"]}
            mb={4}
            mt={4}
            size="lg"
            variant="enclosed"
          >
            <Accordion.Item key="standardFields" value="standardFields">
              <Accordion.ItemTrigger>{translate("connections.form.standardFields")}</Accordion.ItemTrigger>
              <Accordion.ItemContent>
                <StandardFields control={control} standardFields={standardFields} />
              </Accordion.ItemContent>
            </Accordion.Item>
            <FlexibleForm
              flexibleFormDefaultSection={translate("connections.form.extraFields")}
              initialParamsDict={paramsDic}
              key={selectedConnType}
              setError={setFormErrors}
              subHeader={isEditMode ? translate("connections.form.helperTextForRedactedFields") : undefined}
            />
            <Accordion.Item key="extraJson" value="extraJson">
              <Accordion.ItemTrigger cursor="button">
                {translate("connections.form.extraFieldsJson")}
              </Accordion.ItemTrigger>
              <Accordion.ItemContent>
                <Controller
                  control={control}
                  name="extra"
                  render={({ field }) => (
                    <Field.Root invalid={Boolean(errors.conf)}>
                      <JsonEditor
                        {...field}
                        onBlur={() => {
                          field.onChange(validateAndPrettifyJson(field.value));
                        }}
                      />
                      {Boolean(errors.conf) ? <Field.ErrorText>{errors.conf}</Field.ErrorText> : undefined}
                      {isEditMode ? (
                        <Field.HelperText>
                          {translate("connections.form.helperTextForRedactedFields")}
                        </Field.HelperText>
                      ) : undefined}
                    </Field.Root>
                  )}
                />
              </Accordion.ItemContent>
            </Accordion.Item>
          </Accordion.Root>
        ) : undefined}
      </VStack>
      <ErrorAlert error={error} />
      <Box as="footer" display="flex" justifyContent="flex-end" mr={3} mt={4}>
        <HStack w="full">
          <Spacer />
          <Button
            colorPalette="brand"
            disabled={
              Boolean(errors.conf) || formErrors || isPending || !isValid || (!isDirty && !isExtraFieldsDirty)
            }
            onClick={() => void handleSubmit(onSubmit)()}
          >
            <FiSave /> {translate("formActions.save")}
          </Button>
        </HStack>
      </Box>
    </>
  );
};

export default ConnectionForm;
