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
import { Button, Box, Spacer, HStack, Accordion, Text } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiSend } from "react-icons/fi";
import { useSearchParams } from "react-router-dom";

import type { HITLDetail, TaskInstanceResponse } from "openapi/requests/types.gen";
import { FlexibleForm } from "src/components/FlexibleForm/FlexibleForm";
import Time from "src/components/Time";
import { useParamStore } from "src/queries/useParamStore";
import { useUpdateHITLDetail } from "src/queries/useUpdateHITLDetail";
import { DEFAULT_DATETIME_FORMAT } from "src/utils/datetimeUtils";
import { getHITLParamsDict, getHITLFormData, getPreloadHITLFormData } from "src/utils/hitl";

type HITLResponseFormProps = {
  readonly hitlDetail: {
    task_instance: TaskInstanceResponse;
  } & Omit<HITLDetail, "task_instance">;
};

const isHighlightOption = (option: string, hitlDetail: HITLDetail, preloadedHITLOptions: Array<string>) => {
  // preload's priority is higher than default
  const defaultOptions = preloadedHITLOptions.length > 0 ? preloadedHITLOptions : hitlDetail.defaults;

  const isSelected = hitlDetail.chosen_options?.includes(option) && Boolean(hitlDetail.response_received);
  const isDefault = defaultOptions?.includes(option) && !Boolean(hitlDetail.response_received);

  // highlight if:
  // 1. the option is selected and the response is received
  // 2. the option is in default options and the response is not received
  // 3. the option is not selected and the response is not received and there is no default options
  return isSelected ?? isDefault ?? !Boolean(hitlDetail.defaults);
};

export const HITLResponseForm = ({ hitlDetail }: HITLResponseFormProps) => {
  const { t: translate } = useTranslation("hitl");
  const [errors, setErrors] = useState<boolean>(false);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const { paramsDict } = useParamStore("hitl");
  const [searchParams] = useSearchParams();
  const { preloadedHITLOptions } = getPreloadHITLFormData(searchParams, hitlDetail);

  const isApprovalTask =
    hitlDetail.options.includes("Approve") &&
    hitlDetail.options.includes("Reject") &&
    hitlDetail.options.length === 2;

  const shouldRenderOptionButton =
    hitlDetail.options.length < 4 && !hitlDetail.multiple && preloadedHITLOptions.length === 0;

  const isPending = hitlDetail.task_instance.state === "deferred";

  const { updateHITLResponse } = useUpdateHITLDetail({
    dagId: hitlDetail.task_instance.dag_id,
    dagRunId: hitlDetail.task_instance.dag_run_id,
    mapIndex: hitlDetail.task_instance.map_index,
    taskId: hitlDetail.task_instance.task_id,
  });

  const handleSubmit = (option?: string) => {
    if (errors || isSubmitting) {
      return;
    }

    setIsSubmitting(true);

    try {
      const formData = getHITLFormData(paramsDict, option);

      updateHITLResponse(formData);
    } catch {
      setErrors(true);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <Box mt={4}>
      {hitlDetail.response_received ? (
        <Text color="fg.muted" fontSize="sm">
          {translate("response.received")}
          <Time datetime={hitlDetail.response_at} format={DEFAULT_DATETIME_FORMAT} />
        </Text>
      ) : undefined}
      <Accordion.Root
        defaultValue={[hitlDetail.subject]}
        mb={4}
        mt={4}
        overflow="visible"
        size="lg"
        variant="enclosed"
      >
        <FlexibleForm
          disabled={!isPending || hitlDetail.response_received}
          flexFormDescription={hitlDetail.body ?? undefined}
          flexibleFormDefaultSection={hitlDetail.subject}
          initialParamsDict={{
            paramsDict: getHITLParamsDict(hitlDetail, translate, searchParams),
          }}
          isHITL
          key={hitlDetail.subject}
          namespace="hitl"
          setError={setErrors}
        />
      </Accordion.Root>

      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <HStack w="full">
          <Spacer />
          {shouldRenderOptionButton || isApprovalTask ? (
            hitlDetail.options.map((option) => (
              <Button
                colorPalette={isHighlightOption(option, hitlDetail, preloadedHITLOptions) ? "brand" : "gray"}
                disabled={errors || isSubmitting || !isPending || hitlDetail.response_received}
                key={option}
                onClick={() => handleSubmit(option)}
                variant={isHighlightOption(option, hitlDetail, preloadedHITLOptions) ? "solid" : "subtle"}
              >
                {option}
              </Button>
            ))
          ) : hitlDetail.response_received ? undefined : (
            <Button
              colorPalette="brand"
              disabled={errors || isSubmitting}
              loading={isSubmitting}
              onClick={() => handleSubmit()}
            >
              <FiSend /> {translate("response.respond")}
            </Button>
          )}
        </HStack>
      </Box>
    </Box>
  );
};
