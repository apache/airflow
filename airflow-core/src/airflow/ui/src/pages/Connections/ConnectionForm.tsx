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
import { Accordion, Box, Button, HStack, Spacer, Span, VStack } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { FiSave } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { FlexibleForm } from "src/components/FlexibleForm";
import { useConnectionTypeMeta } from "src/queries/useConnectionTypeMeta";
import type { ParamsSpec } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";

import { ConnectionIdField } from "./ConnectionIdField";
import ConnectionStandardFields from "./ConnectionStandardFields";
import { ConnectionTypeField } from "./ConnectionTypeField";
import type { ConnectionBody } from "./Connections";
import { ExtraFieldsJsonSection } from "./ExtraFieldsJsonSection";

type ConnectionFormProps = {
  readonly error: unknown;
  readonly initialConnection: ConnectionBody;
  readonly isEditMode?: boolean;
  readonly isPending: boolean;
  readonly mutateConnection: (requestBody: ConnectionBody) => void;
};

export const ConnectionForm = ({
  error,
  initialConnection,
  isEditMode = false,
  isPending,
  mutateConnection,
}: ConnectionFormProps) => {
  const [jsonError, setJsonError] = useState<string | undefined>();
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
  const selectedConnType = watch("conn_type");
  const standardFields = connectionTypeMeta[selectedConnType]?.standard_fields ?? {};
  const paramsDic = {
    paramsDict: connectionTypeMeta[selectedConnType]?.extra_fields ?? ({} as ParamsSpec),
  };

  const [formErrors, setFormErrors] = useState(false);

  useEffect(() => {
    reset((prevValues) => ({
      ...initialConnection,
      conn_type: selectedConnType,
      connection_id: prevValues.connection_id,
    }));
    setConf(JSON.stringify(JSON.parse(initialConnection.extra), undefined, 2));
  }, [selectedConnType, reset, initialConnection, setConf]);

  useEffect(() => {
    reset((prevValues) => ({
      ...prevValues,
      extra,
    }));
  }, [extra, reset, setConf]);

  const onSubmit = (data: ConnectionBody) => {
    mutateConnection(data);
  };

  const isExtraFieldsDirty = (() => {
    try {
      const initialParsed = JSON.parse(initialConnection.extra) as Record<string, unknown>;
      const currentParsed = JSON.parse(extra) as Record<string, unknown>;

      return JSON.stringify(initialParsed) !== JSON.stringify(currentParsed);
    } catch {
      return extra !== initialConnection.extra;
    }
  })();

  const validateAndPrettifyJson = (value: string) => {
    try {
      if (value.trim() === "") {
        setJsonError(undefined);

        return value;
      }
      const parsedJson = JSON.parse(value) as Record<string, unknown>;

      if (typeof parsedJson !== "object" || Array.isArray(parsedJson)) {
        throw new TypeError('extra fields must be a valid JSON object (e.g., {"key": "value"})');
      }

      setJsonError(undefined);
      const formattedJson = JSON.stringify(parsedJson, undefined, 2);

      if (formattedJson !== extra) {
        setConf(formattedJson);
      }

      return formattedJson;
    } catch (error_) {
      const errorMessage = error_ instanceof Error ? error_.message : translate("common:error.unknown");

      setJsonError(`Invalid JSON format: ${errorMessage}`);

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
        <ConnectionIdField control={control} isDisabled={Boolean(initialConnection.connection_id)} />
        <ConnectionTypeField control={control} isLoading={isMetaPending} options={connTypesOptions} />

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
              <Accordion.ItemTrigger>
                <Span flex="1">{translate("connections.form.standardFields")}</Span>
                <Accordion.ItemIndicator />
              </Accordion.ItemTrigger>
              <Accordion.ItemContent>
                <Accordion.ItemBody>
                  <ConnectionStandardFields control={control} standardFields={standardFields} />
                </Accordion.ItemBody>
              </Accordion.ItemContent>
            </Accordion.Item>
            <FlexibleForm
              flexibleFormDefaultSection={translate("connections.form.extraFields")}
              initialParamsDict={paramsDic}
              key={selectedConnType}
              setError={setFormErrors}
              subHeader={isEditMode ? translate("connections.form.helperTextForRedactedFields") : undefined}
            />
            <ExtraFieldsJsonSection
              control={control}
              error={jsonError}
              isEditMode={isEditMode}
              onBlur={validateAndPrettifyJson}
            />
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
              Boolean(jsonError) || formErrors || isPending || !isValid || (!isDirty && !isExtraFieldsDirty)
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
