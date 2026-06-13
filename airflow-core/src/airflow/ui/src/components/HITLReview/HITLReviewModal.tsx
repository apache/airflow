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
import { Box, Heading, HStack, VStack } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiChevronLeft, FiChevronRight } from "react-icons/fi";

import type { HITLDetail } from "openapi/requests/types.gen.ts";
import { HITLReviewDetail } from "src/components/HITLReview/HITLReviewDetail.tsx";
import { IconButton } from "src/components/ui";
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

  const { hasNext, hasPrevious, onNext, onPrevious, onSelect, selectedDetail } = useHITLReviewModalSelection({
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
      size="xl"
      unmountOnExit
    >
      <Dialog.Content backdrop maxW="1440px" p={4}>
        <Dialog.Header>
          <HStack justifyContent="space-between" pr={8} width="100%">
            <Heading flexShrink={0} size="md">
              {translate("requiredAction_other")}
            </Heading>
            <HStack gap={2}>
              {headerAction}
              {shouldShowCompletedHitl ? (
                <ButtonGroupToggle<HITLReviewFilterMode>
                  onChange={setSelectedFilter}
                  options={HITL_REVIEW_FILTER_OPTIONS.map((option) => ({
                    label: translate(option.labelKey),
                    value: option.value,
                  }))}
                  size="xs"
                  value={selectedFilter}
                />
              ) : undefined}
              <HStack gap={1}>
                <IconButton
                  disabled={!hasPrevious}
                  label={translate("review.navigation.previous")}
                  onClick={onPrevious}
                >
                  <FiChevronLeft />
                </IconButton>
                <IconButton disabled={!hasNext} label={translate("review.navigation.next")} onClick={onNext}>
                  <FiChevronRight />
                </IconButton>
              </HStack>
            </HStack>
          </HStack>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          <HStack
            alignItems="stretch"
            flexDirection={{ base: "column", lg: "row" }}
            gap={{ base: 3, lg: 0 }}
            height="100%"
            width="100%"
          >
            <Box
              flex={{ base: 1, lg: 2 }}
              minW={0}
              overflowX="hidden"
              overflowY="auto"
              pl={2}
              position="relative"
              pr={{ base: 2, lg: 4 }}
              py={2}
              zIndex={2}
            >
              <VStack alignItems="stretch" gap={4} width="100%">
                <HITLReviewListSection
                  details={pendingHitl.data}
                  heading={translate("review.list.pendingRequiredActions", {
                    count: pendingHitl.data.length,
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
                      count: completedHitl.data.length,
                    })}
                    isError={completedHitl.isError}
                    isLoading={completedHitl.isLoading}
                    onSelect={onSelect}
                    selectedDetail={selectedDetail}
                  />
                ) : null}
              </VStack>
            </Box>
            <Box
              bg="bg"
              borderColor="border"
              borderRadius="md"
              borderWidth={1}
              flex={1}
              minW={0}
              overflowY="auto"
              p={3}
              position="relative"
              zIndex={1}
            >
              <HITLReviewDetail detail={selectedDetail} onOpenTask={handleClose} onResponded={onNext} />
            </Box>
          </HStack>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
