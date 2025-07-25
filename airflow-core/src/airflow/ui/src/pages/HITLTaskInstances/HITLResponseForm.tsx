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
import { Button, Box, Spacer, HStack, Accordion } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiSend } from "react-icons/fi";

import { useHumanInTheLoopServiceGetMappedTiHitlDetail } from "openapi/queries";
import { FlexibleForm } from "src/components/FlexibleForm";
import { useHITLResponseState } from "src/pages/HITLTaskInstances/useHITLResponseState";
import { useParamStore } from "src/queries/useParamStore";
import { useUpdateHITLDetail } from "src/queries/useUpdateHITLDetail";
import { getHITLParamsDict, getHITLFormData } from "src/utils/hitl";

export const HITLResponseForm = () => {
  const { t: translate } = useTranslation();
  const [errors, setErrors] = useState<boolean>(false);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const { paramsDict } = useParamStore();
  const { taskInstance } = useHITLResponseState();
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
      const formData = getHITLFormData(paramsDict);

      updateHITLResponse(formData);
    } catch {
      setErrors(true);
    } finally {
      setIsSubmitting(false);
    }
  };

  if (!hitlDetail || !taskInstance) {
    return undefined;
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
          disabled={hitlDetail.response_received}
          flexFormDescription={hitlDetail.body ?? undefined}
          flexibleFormDefaultSection={hitlDetail.subject}
          initialParamsDict={{
            paramsDict: getHITLParamsDict(hitlDetail, translate),
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
