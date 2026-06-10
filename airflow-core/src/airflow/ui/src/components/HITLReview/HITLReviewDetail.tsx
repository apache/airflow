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
import { Button, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";

import type { HITLDetail } from "openapi/requests/types.gen.ts";
import { HITLResponseForm } from "src/pages/HITLTaskInstances/HITLResponseForm.tsx";
import { getTaskInstanceLink } from "src/utils/links.ts";

import { HITLReviewDetailSummary } from "./HITLReviewDetailSummary.tsx";

export const HITLReviewDetail = ({
  detail,
  onNavigate,
  onResponded,
}: {
  readonly detail?: HITLDetail;
  readonly onNavigate: () => void;
  readonly onResponded: () => void;
}) => {
  const { t: translate } = useTranslation("hitl");

  if (detail === undefined) {
    return (
      <VStack alignItems="center" gap={2} justifyContent="center" minH="240px" width="100%">
        <Text color="fg.muted" fontSize="sm">
          {translate("review.selectRequiredAction")}
        </Text>
      </VStack>
    );
  }

  const ti = detail.task_instance;
  const taskLink = `${getTaskInstanceLink(ti)}/required_actions`;

  return (
    <VStack alignItems="stretch" gap={4} width="100%">
      <HITLReviewDetailSummary detail={detail} />

      <Button alignSelf="flex-end" asChild size="sm" variant="outline">
        <Link onClick={onNavigate} to={taskLink}>
          {translate("review.openTask")}
        </Link>
      </Button>

      <HITLResponseForm hitlDetail={detail} namespace={ti.id} onResponded={onResponded} />
    </VStack>
  );
};
