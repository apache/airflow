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

/* eslint-disable i18next/no-literal-string */
import { Button, Box, Spacer, HStack, Text, Accordion } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiSend } from "react-icons/fi";

import { useHumanInTheLoopServiceGetMappedTiHitlDetail } from "openapi/queries";
import { FlexibleForm } from "src/components/FlexibleForm";
import { useHITLResponseState } from "src/pages/HITLTaskInstances/useHITLResponseState";
import { useParamStore } from "src/queries/useParamStore";
import { useUpdateHITLDetail } from "src/queries/useUpdateHITLDetail";

export type HITLResponseParams = {
  chosen_options?: Array<string>;
  params_input?: Record<string, unknown>;
};

enum HITLOperatorName {
  ApprovalOperator = "ApprovalOperator",
  HITLEntryOperator = "HITLEntryOperator",
  HITLOperator = "HITLOperator",
}

export const HITLResponseForm = () => {
  const { t: translate } = useTranslation();
  const [errors, setErrors] = useState<boolean>(false);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const { paramsDict } = useParamStore();
  const { onClose, taskInstance } = useHITLResponseState();
  const { updateHITLResponse } = useUpdateHITLDetail({
    dagId: taskInstance?.dag_id ?? "",
    dagRunId: taskInstance?.dag_run_id ?? "",
    mapIndex: taskInstance?.map_index ?? -1,
    taskId: taskInstance?.task_id ?? "",
  });

  const { data: hitlDetail } = useHumanInTheLoopServiceGetMappedTiHitlDetail({
    dagId: taskInstance?.dag_id ?? "",
    dagRunId: taskInstance?.dag_run_id ?? "",
    mapIndex: taskInstance?.map_index ?? -1,
    taskId: taskInstance?.task_id ?? "",
  });

  const handleSubmit = () => {
    if (errors || isSubmitting) {
      return;
    }

    setIsSubmitting(true);

    try {
      const chosenOptionsValue = paramsDict.chosen_options?.value;
      let chosenOptions: Array<string> = [];

      if (Array.isArray(chosenOptionsValue)) {
        chosenOptions = chosenOptionsValue.filter(
          (value): value is string => value !== null && value !== undefined,
        );
      } else if (typeof chosenOptionsValue === "string" && chosenOptionsValue) {
        chosenOptions = [chosenOptionsValue];
      }

      const formData: HITLResponseParams = {
        chosen_options: chosenOptions,
        params_input: paramsDict.params_input?.value as Record<string, unknown>,
      };

      updateHITLResponse(formData);
      onClose();
    } catch {
      setErrors(true);
    } finally {
      setIsSubmitting(false);
    }
  };

  if (!hitlDetail || !taskInstance) {
    return (
      <Box mt={8}>
        <Text>No HITL detail found</Text>
      </Box>
    );
  }

  return (
    <Box mt={8}>
      <Accordion.Root
        collapsible
        defaultValue={[hitlDetail.subject]}
        mb={4}
        mt={4}
        size="lg"
        variant="enclosed"
      >
        <FlexibleForm
          flexFormDescription={hitlDetail.body ?? undefined}
          flexibleFormDefaultSection={hitlDetail.subject}
          initialParamsDict={{
            paramsDict: {
              chosen_options: {
                description: translate("hitl:response.optionsDescription"),
                schema: {
                  const: undefined,
                  description_md: translate("hitl:response.optionsDescription"),
                  enum:
                    taskInstance.operator === HITLOperatorName.HITLEntryOperator
                      ? undefined
                      : hitlDetail.options,
                  examples: undefined,
                  format: undefined,
                  items: hitlDetail.multiple ? { type: "string" } : undefined,
                  maximum: undefined,
                  maxLength: undefined,
                  minimum: undefined,
                  minLength: undefined,
                  section: undefined,
                  title: translate("hitl:response.optionsLabel"),
                  type: hitlDetail.multiple ? "array" : "string",
                  values_display: undefined,
                },
                value: hitlDetail.response_received
                  ? hitlDetail.multiple
                    ? hitlDetail.chosen_options
                    : hitlDetail.chosen_options?.[0]
                  : hitlDetail.defaults,
              },
              params_input: {
                description: translate("hitl:response.paramsDescription"),
                schema: {
                  const: undefined,
                  description_md: translate("hitl:response.paramsDescription"),
                  enum: undefined,
                  examples: undefined,
                  format: undefined,
                  items: undefined,
                  maximum: undefined,
                  maxLength: undefined,
                  minimum: undefined,
                  minLength: undefined,
                  section: undefined,
                  title: translate("hitl:response.paramsLabel"),
                  type: "object",
                  values_display: hitlDetail.params as Record<string, string>,
                },
                value: hitlDetail.params,
              },
            },
          }}
          key={hitlDetail.subject}
          setError={setErrors}
        />
      </Accordion.Root>
      {hitlDetail.response_received ? undefined : (
        <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
          <HStack w="full">
            <Spacer />
            <Button
              colorPalette="blue"
              disabled={errors || isSubmitting}
              loading={isSubmitting}
              onClick={handleSubmit}
            >
              <FiSend /> {translate("hitl:response.button")}
            </Button>
          </HStack>
        </Box>
      )}
    </Box>
  );
};
