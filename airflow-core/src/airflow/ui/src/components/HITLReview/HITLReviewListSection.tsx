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
import { Heading, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { HITLDetail } from "openapi/requests/types.gen.ts";

import { HITLReviewList } from "./HITLReviewList.tsx";

export const HITLReviewListSection = ({
  details,
  heading,
  isError = false,
  isLoading = false,
  onSelect,
  selectedDetail,
}: {
  readonly details?: Array<HITLDetail>;
  readonly heading: string;
  readonly isError?: boolean;
  readonly isLoading?: boolean;
  readonly onSelect: (selection: HITLDetail) => void;
  readonly selectedDetail?: HITLDetail;
}) => {
  const { t: translate } = useTranslation("hitl");

  return (
    <VStack alignItems="stretch">
      <Heading size="md">{heading}</Heading>
      <VStack alignItems="stretch" borderRadius="md" borderWidth={1} overflowX="auto">
        {isLoading ? (
          <Text color="fg.muted">{translate("review.list.loadingActions")}</Text>
        ) : isError ? (
          <Text color="fg.error">{translate("review.list.loadError")}</Text>
        ) : (
          <HITLReviewList details={details ?? []} onSelect={onSelect} selectedDetail={selectedDetail} />
        )}
      </VStack>
    </VStack>
  );
};
