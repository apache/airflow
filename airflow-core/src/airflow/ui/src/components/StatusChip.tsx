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

import { Badge, HStack } from "@chakra-ui/react";

import { Popover, RouterLink } from "src/system-components";

/** Highest severity first; callers render chips in this order. */
export type StatusChipSeverity = "error" | "info" | "warning";

type Props = {
  /** Detail and actions shown on click. Omit for a chip that only states status. */
  readonly children?: ReactNode;
  readonly icon?: ReactNode;
  readonly label: string;
  /** Click handler for a chip whose detail lives in a modal rather than a popover. */
  readonly onClick?: () => void;
  readonly severity: StatusChipSeverity;
  /** Destination for a chip whose detail lives on another page. */
  readonly to?: string;
};

export const StatusChip = ({ children, icon, label, onClick, severity, to }: Props) => {
  const content = (
    <HStack gap={1}>
      {icon}
      {label}
    </HStack>
  );
  // Only an error should compete with the object's own state badge for attention.
  const variant = severity === "error" ? "solid" : "subtle";

  if (to !== undefined) {
    return (
      <RouterLink to={to}>
        <Badge colorPalette={severity} cursor="pointer" variant={variant}>
          {content}
        </Badge>
      </RouterLink>
    );
  }

  if (children === undefined) {
    return onClick === undefined ? (
      <Badge colorPalette={severity} variant={variant}>
        {content}
      </Badge>
    ) : (
      <Badge as="button" colorPalette={severity} cursor="pointer" onClick={onClick} variant={variant}>
        {content}
      </Badge>
    );
  }

  return (
    <Popover.Root lazyMount unmountOnExit>
      <Popover.Trigger asChild>
        <Badge as="button" colorPalette={severity} cursor="pointer" variant={variant}>
          {content}
        </Badge>
      </Popover.Trigger>
      <Popover.Content maxW="360px" width="fit-content">
        <Popover.Arrow />
        <Popover.Body>{children}</Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
