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
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { FlexibleForm } from "src/components/FlexibleForm";
import { useParamStore } from "src/queries/useParamStore";
import { useUpdateHITLDetail } from "src/queries/useUpdateHITLDetail";

type HITLResponseFormProps = {
  readonly onClose: () => void;
  readonly taskInstance: TaskInstanceResponse;
};

export type HITLResponseParams = {
  chosen_options?: Array<string>;
  params_input?: Record<string, unknown>;
};

export const HITLResponseForm = ({ onClose, taskInstance }: HITLResponseFormProps) => {
  const { t: translate } = useTranslation();
  const [errors, setErrors] = useState<boolean>(false);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const { paramsDict } = useParamStore();

  const { updateHITLResponse } = useUpdateHITLDetail({
    dagId: taskInstance.dag_id,
    dagRunId: taskInstance.dag_run_id,
    mapIndex: taskInstance.map_index,
    taskId: taskInstance.task_id,
  });

  const { data: hitlDetail } = useHumanInTheLoopServiceGetMappedTiHitlDetail({
    dagId: taskInstance.dag_id,
    dagRunId: taskInstance.dag_run_id,
    mapIndex: taskInstance.map_index,
    taskId: taskInstance.task_id,
  });

  const handleSubmit = () => {
    if (errors || isSubmitting) {
      return;
    }

    setIsSubmitting(true);

    try {
      const formData: HITLResponseParams = {
        chosen_options: [paramsDict.chosen_options?.value as string],
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

  if (!hitlDetail) {
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
                description: translate("hitl:response.responseHelp"),
                schema: {
                  const: undefined,
                  description_md: translate("hitl:response.responseHelp"),
                  enum: hitlDetail.options,
                  examples: undefined,
                  format: undefined,
                  items: undefined,
                  maximum: undefined,
                  maxLength: undefined,
                  minimum: undefined,
                  minLength: undefined,
                  section: undefined,
                  title: translate("hitl:response.responseLabel"),
                  type: "string",
                  values_display: undefined,
                },
                value: hitlDetail.defaults,
              },
              params_input: {
                description: translate("hitl:response.paramsHelp"),
                schema: {
                  const: undefined,
                  description_md: translate("hitl:response.paramsHelp"),
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
    </Box>
  );
};
