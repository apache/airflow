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
import { Box, HStack, Icon, VStack } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiInfo } from "react-icons/fi";

import type { HITLDetail } from "openapi/requests/types.gen.ts";
import { HITLReviewDetail } from "src/components/HITLReview/HITLReviewDetail.tsx";
import { Tooltip } from "src/components/ui";
import { ButtonGroupToggle } from "src/components/ui/ButtonGroupToggle";
import { Dialog } from "src/components/ui/Dialog";

import { HITLReviewListSection } from "./HITLReviewListSection.tsx";
import { useHITLReviewModalSelection } from "./useHITLReviewModalSelection.ts";

enum HITLReviewFilterMode {
  ALL = "all",
  PENDING = "pending",
}

type HITLReviewListState = {
  readonly data: Array<HITLDetail>;
  readonly isError?: boolean;
  readonly isLoading?: boolean;
  readonly total?: number;
};

const HITL_REVIEW_FILTER_OPTIONS: Array<{ labelKey: string; value: HITLReviewFilterMode }> = [
  { labelKey: "filters.response.pending", value: HITLReviewFilterMode.PENDING },
  { labelKey: "filters.response.all", value: HITLReviewFilterMode.ALL },
];

export const HITLReviewModal = ({
  completedHitl,
  headerAction,
  onClose,
  open,
  pendingHitl,
}: {
  readonly completedHitl?: HITLReviewListState;
  readonly headerAction?: ReactNode;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly pendingHitl: HITLReviewListState;
}) => {
  const { t: translate } = useTranslation("hitl");
  const [selectedFilter, setSelectedFilter] = useState<HITLReviewFilterMode>(HITLReviewFilterMode.PENDING);
  const shouldShowCompletedHitl = completedHitl !== undefined;
  const visibleHitls =
    shouldShowCompletedHitl && selectedFilter === HITLReviewFilterMode.ALL
      ? [...pendingHitl.data, ...completedHitl.data]
      : pendingHitl.data;

  const { onNext, onSelect, selectedDetail } = useHITLReviewModalSelection({
    hitlDetails: visibleHitls,
  });
  const handleClose = () => {
    setSelectedFilter(HITLReviewFilterMode.PENDING);
    onClose();
  };

  return (
    <Dialog.Root
      lazyMount
      onOpenChange={(event) => {
        if (!event.open) {
          handleClose();
        }
      }}
      open={open}
      scrollBehavior="inside"
      unmountOnExit
    >
      <Dialog.Content maxW="1440px" p={4}>
        <Dialog.Header>
          <HStack justifyContent="space-between" overflowX="auto" width="100%">
            <HStack flexShrink={0} gap={1}>
              <Dialog.Title>{translate("requiredAction_other")}</Dialog.Title>
              <Tooltip content={translate("review.pageLimitHint")}>
                <Icon color="fg.muted">
                  <FiInfo />
                </Icon>
              </Tooltip>
            </HStack>
            <HStack gap={3}>
              {headerAction}
              {shouldShowCompletedHitl ? (
                <ButtonGroupToggle<HITLReviewFilterMode>
                  onChange={setSelectedFilter}
                  options={HITL_REVIEW_FILTER_OPTIONS.map((option) => ({
                    label: translate(option.labelKey),
                    value: option.value,
                  }))}
                  value={selectedFilter}
                />
              ) : undefined}
            </HStack>
          </HStack>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          <HStack alignItems="stretch" flexDirection={{ base: "column", lg: "row" }} gap={6}>
            <Box flex="2">
              <VStack alignItems="stretch" gap={4}>
                <HITLReviewListSection
                  details={pendingHitl.data}
                  heading={translate("review.list.pendingRequiredActions", {
                    count: pendingHitl.total ?? pendingHitl.data.length,
                  })}
                  isError={pendingHitl.isError}
                  isLoading={pendingHitl.isLoading}
                  onSelect={onSelect}
                  selectedDetail={selectedDetail}
                />
                {shouldShowCompletedHitl && selectedFilter === HITLReviewFilterMode.ALL ? (
                  <HITLReviewListSection
                    details={completedHitl.data}
                    heading={translate("review.list.completedRequiredActions", {
                      count: completedHitl.total ?? completedHitl.data.length,
                    })}
                    isError={completedHitl.isError}
                    isLoading={completedHitl.isLoading}
                    onSelect={onSelect}
                    selectedDetail={selectedDetail}
                  />
                ) : null}
              </VStack>
            </Box>
            <Box borderRadius="md" borderWidth={1} flex="1" mt={8} p={3}>
              <HITLReviewDetail detail={selectedDetail} onOpenTask={handleClose} onResponded={onNext} />
            </Box>
          </HStack>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
