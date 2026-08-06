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
import {
  Badge,
  Box,
  Button,
  type ButtonProps,
  CloseButton,
  Drawer,
  Flex,
  HStack,
  Portal,
  Text,
  useBreakpointValue,
  VisuallyHidden,
  VStack,
} from "@chakra-ui/react";
import { forwardRef, type ReactNode, type RefObject } from "react";
import { FiFilter } from "react-icons/fi";

import { Popover } from "src/components/ui";

export type SearchAndFilterLabels = {
  readonly activeFilterCount: (count: number) => string;
  readonly clearFilters: string;
  readonly closeFilters: string;
  readonly filterButton: string;
  readonly filterTitle: string;
};

type Props = {
  readonly activeFilterCount: number;
  readonly activeFilters?: ReactNode;
  readonly activeFiltersTestId?: string;
  readonly children: ReactNode;
  readonly finalFocusEl?: () => HTMLElement | null;
  readonly initialFocusEl?: () => HTMLElement | null;
  readonly labels: SearchAndFilterLabels;
  readonly onClearFilters: () => void;
  readonly onOpenChange: (open: boolean) => void;
  readonly open: boolean;
  readonly searchControl: ReactNode;
  readonly triggerRef: RefObject<HTMLButtonElement | null>;
  readonly triggerTestId?: string;
};

type DisclosureProps = Pick<
  Props,
  | "activeFilterCount"
  | "children"
  | "finalFocusEl"
  | "initialFocusEl"
  | "labels"
  | "onOpenChange"
  | "open"
  | "triggerRef"
  | "triggerTestId"
> & {
  readonly onClearFilters: () => void;
};

type FilterTriggerProps = ButtonProps &
  Pick<DisclosureProps, "activeFilterCount" | "labels" | "triggerTestId">;

const FilterTrigger = forwardRef<HTMLButtonElement, FilterTriggerProps>(
  ({ activeFilterCount, labels, triggerTestId, ...buttonProps }, ref) => (
    <Button
      {...buttonProps}
      aria-label={`${labels.filterButton}, ${labels.activeFilterCount(activeFilterCount)}`}
      data-testid={triggerTestId}
      ref={ref}
      variant="outline"
    >
      <FiFilter />
      {labels.filterButton}
      <Badge aria-hidden="true" colorPalette={activeFilterCount > 0 ? "blue" : "gray"} variant="subtle">
        {activeFilterCount}
      </Badge>
    </Button>
  ),
);

const FilterFooter = ({
  activeFilterCount,
  labels,
  onClearFilters,
}: Pick<DisclosureProps, "activeFilterCount" | "labels" | "onClearFilters">) => (
  <HStack justify="space-between" width="full">
    <Text color="fg.muted" fontSize="sm">
      {labels.activeFilterCount(activeFilterCount)}
    </Text>
    <Button
      colorPalette="gray"
      disabled={activeFilterCount === 0}
      onClick={onClearFilters}
      size="sm"
      variant="ghost"
    >
      {labels.clearFilters}
    </Button>
  </HStack>
);

const DesktopDisclosure = ({
  activeFilterCount,
  children,
  finalFocusEl,
  initialFocusEl,
  labels,
  onClearFilters,
  onOpenChange,
  open,
  triggerRef,
  triggerTestId,
}: DisclosureProps) => (
  <Popover.Root
    finalFocusEl={finalFocusEl}
    initialFocusEl={initialFocusEl}
    lazyMount
    modal
    onOpenChange={(details) => onOpenChange(details.open)}
    open={open}
    positioning={{ placement: "bottom-end" }}
    restoreFocus
    unmountOnExit
  >
    <Popover.Trigger asChild>
      <FilterTrigger
        activeFilterCount={activeFilterCount}
        labels={labels}
        ref={triggerRef}
        triggerTestId={triggerTestId}
      />
    </Popover.Trigger>
    <Popover.Content
      display="flex"
      flexDirection="column"
      maxHeight="min(720px, calc(100vh - 120px))"
      maxWidth="calc(100vw - 32px)"
      overflow="hidden"
      width="680px"
    >
      <Popover.Arrow />
      <Popover.Header>
        <HStack justify="space-between">
          <Popover.Title fontWeight="semibold">{labels.filterTitle}</Popover.Title>
          <Popover.CloseTrigger aria-label={labels.closeFilters} />
        </HStack>
      </Popover.Header>
      <Popover.Body flex="1" overflowY="auto">
        {children}
      </Popover.Body>
      <Box borderTopWidth="1px" px={4} py={3}>
        <FilterFooter activeFilterCount={activeFilterCount} labels={labels} onClearFilters={onClearFilters} />
      </Box>
    </Popover.Content>
  </Popover.Root>
);

const MobileDisclosure = ({
  activeFilterCount,
  children,
  finalFocusEl,
  initialFocusEl,
  labels,
  onClearFilters,
  onOpenChange,
  open,
  triggerRef,
  triggerTestId,
}: DisclosureProps) => (
  <Drawer.Root
    finalFocusEl={finalFocusEl}
    initialFocusEl={initialFocusEl}
    lazyMount
    onOpenChange={(details) => onOpenChange(details.open)}
    open={open}
    placement="bottom"
    restoreFocus
    size="full"
    unmountOnExit
  >
    <Drawer.Trigger asChild>
      <FilterTrigger
        activeFilterCount={activeFilterCount}
        labels={labels}
        ref={triggerRef}
        triggerTestId={triggerTestId}
      />
    </Drawer.Trigger>
    <Portal>
      <Drawer.Backdrop />
      <Drawer.Positioner>
        <Drawer.Content>
          <Drawer.Header>
            <Drawer.Title>{labels.filterTitle}</Drawer.Title>
          </Drawer.Header>
          <Drawer.CloseTrigger asChild>
            <CloseButton aria-label={labels.closeFilters} />
          </Drawer.CloseTrigger>
          <Drawer.Body>{children}</Drawer.Body>
          <Drawer.Footer borderTopWidth="1px">
            <FilterFooter
              activeFilterCount={activeFilterCount}
              labels={labels}
              onClearFilters={onClearFilters}
            />
          </Drawer.Footer>
        </Drawer.Content>
      </Drawer.Positioner>
    </Portal>
  </Drawer.Root>
);

export const SearchAndFilter = ({
  activeFilterCount,
  activeFilters,
  activeFiltersTestId,
  children,
  finalFocusEl,
  initialFocusEl,
  labels,
  onClearFilters,
  onOpenChange,
  open,
  searchControl,
  triggerRef,
  triggerTestId,
}: Props) => {
  const isNarrow = useBreakpointValue({ base: true, md: false }) ?? false;
  const getFinalFocusEl = () => finalFocusEl?.() ?? triggerRef.current;
  const clearFilters = () => {
    onClearFilters();
    if (open) {
      onOpenChange(false);
    } else {
      triggerRef.current?.focus();
    }
  };
  const disclosureProps: DisclosureProps = {
    activeFilterCount,
    children,
    finalFocusEl: getFinalFocusEl,
    initialFocusEl,
    labels,
    onClearFilters: clearFilters,
    onOpenChange,
    open,
    triggerRef,
    triggerTestId,
  };

  return (
    <VStack align="stretch" gap={2}>
      <VisuallyHidden asChild>
        <output aria-live="polite">{labels.activeFilterCount(activeFilterCount)}</output>
      </VisuallyHidden>
      <Flex
        align={{ base: "stretch", md: "center" }}
        direction={{ base: "column", md: "row" }}
        gap={2}
        maxWidth={{ base: "full", md: "960px" }}
        width="full"
      >
        <Box flex={1} minWidth={0}>
          {searchControl}
        </Box>
        {isNarrow ? <MobileDisclosure {...disclosureProps} /> : <DesktopDisclosure {...disclosureProps} />}
      </Flex>
      {activeFilterCount > 0 ? (
        <HStack align="center" data-testid={activeFiltersTestId} flexWrap="wrap" gap={2}>
          {activeFilters}
          <Button colorPalette="gray" onClick={clearFilters} size="sm" variant="ghost">
            {labels.clearFilters}
          </Button>
        </HStack>
      ) : undefined}
    </VStack>
  );
};
