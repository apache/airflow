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

import { Badge } from "@chakra-ui/react";

import { IconButton, Tooltip } from "src/system-components";

/** Highest severity first; callers render chips in this order. */
export type StatusChipSeverity = "error" | "info" | "warning";

type Props = {
  readonly icon: ReactNode;
  /** Names the chip for its tooltip and for assistive tech, since nothing is written on it. */
  readonly label: string;
  /** Opens the chip's detail. Omit for a chip that only states status. */
  readonly onClick?: () => void;
  readonly severity: StatusChipSeverity;
};

/**
 * A compact status marker. Icon only: these sit in tight places, where a worded chip pushes
 * everything else around, so the wording lives in the tooltip.
 *
 * One that opens something is a real icon button, so it carries button semantics and the same
 * hit area and hover as the actions beside it; one that only reports is an inert badge.
 */
export const StatusChip = ({ icon, label, onClick, severity }: Props) =>
  onClick === undefined ? (
    // Portalled, like IconButton's own: these sit in containers that clip what overflows them.
    // Sized to match the button form so the two read as the same marker at different severities.
    <Tooltip content={label} portalled>
      <Badge
        alignItems="center"
        aria-label={label}
        boxSize={8}
        colorPalette={severity}
        display="inline-flex"
        fontSize="md"
        justifyContent="center"
        px={0}
        variant="subtle"
      >
        {icon}
      </Badge>
    </Tooltip>
  ) : (
    <IconButton colorPalette={severity} label={label} onClick={onClick} size="sm" variant="solid">
      {icon}
    </IconButton>
  );
