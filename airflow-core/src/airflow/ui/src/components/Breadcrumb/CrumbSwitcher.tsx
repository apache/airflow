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
import type { ReactNode } from "react";

import { Box, type BoxProps } from "@chakra-ui/react";
import { BsChevronExpand } from "react-icons/bs";

import { IconButton, Popover, Tooltip } from "src/system-components";

import { CrumbDivider, CrumbGroup, CrumbLink, CrumbStatic } from "./Crumb";
import { CUT_PADDING, type CrumbShape, crumbButtonStyles, getWedgePadding } from "./segment";

type Props = {
  readonly children: ReactNode;
  /** Width of the panel. The default suits a search; a short fixed list wants to shrink to fit. */
  readonly contentWidth?: BoxProps["width"];
  /** Names the chevron for the tooltip and for assistive technology. */
  readonly label: string;
  readonly onOpenChange: (open: boolean) => void;
  readonly open: boolean;
  readonly search: ReactNode;
  readonly shape: CrumbShape;
  readonly testId: string;
  /** Where the naming half goes. Omitted for a level whose only destination is the page it is on. */
  readonly to?: string;
};

/**
 * A breadcrumb level that also switches between its siblings: a two-part control whose left half
 * names the level (and navigates to it, where that leads anywhere) and whose right half opens a
 * search, so switching happens where the
 * current one is named. Open state stays at the call site, which owns the keyboard shortcut (if
 * the level has one) and the search the panel holds.
 */
export const CrumbSwitcher = ({
  children,
  contentWidth = "sm",
  label,
  onOpenChange,
  open,
  search,
  shape,
  testId,
  to,
}: Props) => (
  <Popover.Root
    lazyMount
    onOpenChange={(event) => onOpenChange(event.open)}
    open={open}
    positioning={{ placement: "bottom-start" }}
    unmountOnExit
  >
    {/* Anchored to the whole level rather than the chevron: it shares the breadcrumb's start
        edge and its bottom, so the panel drops clear of the bar and lines up with it. */}
    <Popover.Anchor asChild>
      <CrumbGroup shape={shape}>
        {to === undefined ? (
          <CrumbStatic paddingInlineEnd={CUT_PADDING}>{children}</CrumbStatic>
        ) : (
          <CrumbLink paddingInlineEnd={CUT_PADDING} to={to}>
            {children}
          </CrumbLink>
        )}
        <CrumbDivider />
        {/* The tooltip wraps the trigger rather than coming from IconButton's `label`: nesting it
          inside `asChild` leaves its own trigger ref unset, and it renders away from the button. */}
        <Tooltip content={label} disabled={open} portalled>
          <Popover.Trigger asChild>
            <IconButton
              {...crumbButtonStyles}
              {...getWedgePadding(shape)}
              alignSelf="stretch"
              aria-label={label}
              data-testid={testId}
              paddingInlineStart={2}
            >
              <BsChevronExpand />
            </IconButton>
          </Popover.Trigger>
        </Tooltip>
      </CrumbGroup>
    </Popover.Anchor>
    <Popover.Content data-testid={`${testId}-popover`} width={contentWidth}>
      <Box p={2}>{search}</Box>
    </Popover.Content>
  </Popover.Root>
);
