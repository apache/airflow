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
import { Box, Button, type ButtonProps, HStack, type StackProps } from "@chakra-ui/react";
import { type ReactNode, forwardRef } from "react";
import { Link } from "react-router-dom";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";

import { type CrumbShape, crumbButtonStyles, getSegmentStyles, getWedgePadding } from "./segment";

type CrumbStackProps = {
  readonly caption: string;
  /**
   * Whether this level reports a state at all. Levels that do keep the badge's room reserved from
   * the first paint, so the bar does not change width when the value arrives a moment later.
   */
  readonly hasState?: boolean;
  readonly isCurrent?: boolean;
  readonly state?: TaskInstanceState | null;
  readonly value: string;
};

/**
 * A level's caption and value. Everything is a span: a crumb sits inside a link or a button, where
 * only phrasing content is legal.
 */
export const CrumbStack = ({
  caption,
  hasState = false,
  isCurrent = false,
  state,
  value,
}: CrumbStackProps) => (
  <>
    <Box as="span" color="fg.muted" fontSize="xs" lineHeight="1.3">
      {caption}
    </Box>
    <Box alignItems="center" as="span" display="flex" gap={1.5} lineHeight="1.4">
      <Box as="span" fontSize="sm" fontWeight={isCurrent ? "medium" : "normal"} minW={0} truncate>
        {value}
      </Box>
      {hasState ? (
        // Rendered but hidden until the state loads: `display: none` would collapse the badge's
        // width and shift the whole bar. `state ?? null` keeps StateBadge off an undefined palette.
        <StateBadge
          boxSize={4}
          css={{ "& svg": { height: "10px", width: "10px" } }}
          flexShrink={0}
          justifyContent="center"
          minH={0}
          minW={0}
          p={0}
          state={state ?? null}
          visibility={state === undefined ? "hidden" : "visible"}
        />
      ) : undefined}
    </Box>
  </>
);

export const CrumbLink = ({
  children,
  shape,
  to,
  ...rest
}: {
  readonly children: ReactNode;
  readonly shape?: CrumbShape;
  readonly to: string;
} & ButtonProps) => (
  <Button
    asChild
    {...crumbButtonStyles}
    {...(shape === undefined ? undefined : getSegmentStyles(shape))}
    {...getWedgePadding(shape ?? { hasNotch: false, hasPoint: false })}
    {...rest}
  >
    <Link to={to}>{children}</Link>
  </Button>
);

/** The level the page is already on: it goes nowhere, so it is not a button. */
export const CrumbText = ({
  children,
  shape,
}: {
  readonly children: ReactNode;
  readonly shape: CrumbShape;
}) => (
  <Box
    {...getSegmentStyles(shape)}
    {...getWedgePadding(shape)}
    alignItems="flex-start"
    aria-current="page"
    display="flex"
    flexDirection="column"
    gap={0}
    justifyContent="center"
    py={1.5}
  >
    {children}
  </Box>
);

/** Wraps the Dag level's two halves so the pair is cut as a single segment. */
export const CrumbGroup = forwardRef<
  HTMLDivElement,
  { readonly children: ReactNode; readonly shape: CrumbShape } & StackProps
>(({ children, shape, ...rest }, ref) => (
  <HStack {...getSegmentStyles(shape)} alignItems="stretch" gap={0} ref={ref} {...rest}>
    {children}
  </HStack>
));

/**
 * The straight cut inside the Dag level. Thinner than the chevrons between levels, since it divides
 * one control rather than separating two.
 */
export const CrumbDivider = () => <Box alignSelf="stretch" aria-hidden bg="bg" flexShrink={0} width="1px" />;
