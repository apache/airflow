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
import { Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { HITLDetail } from "openapi/requests/types.gen.ts";
import { HITLResponseForm } from "src/pages/HITLTaskInstances/HITLResponseForm.tsx";

import { HITLReviewDetailSummary } from "./HITLReviewDetailSummary.tsx";

export const HITLReviewDetail = ({
  detail,
  onOpenTask,
  onResponded,
}: {
  readonly detail?: HITLDetail;
  readonly onOpenTask: () => void;
  readonly onResponded: () => void;
}) => {
  const { t: translate } = useTranslation("hitl");

  if (detail === undefined) {
    return (
      <VStack py={20}>
        <Text color="fg.muted">{translate("review.detail.selectRequiredAction")}</Text>
      </VStack>
    );
  }

  const ti = detail.task_instance;

  return (
    <VStack alignItems="stretch" gap={4}>
      <HITLReviewDetailSummary detail={detail} onOpenTask={onOpenTask} />

      <HITLResponseForm hitlDetail={detail} key={ti.id} namespace={ti.id} onResponded={onResponded} />
    </VStack>
  );
};
