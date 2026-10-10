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
import { Button } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuUserRoundPen } from "react-icons/lu";

import { RouterLink, Tooltip } from "src/system-components";

import { StateBadge } from "src/components/StateBadge";

import { formatNumber } from "src/utils";

type Props = {
  readonly count: number;
  /** Opens the review modal. Omit when `to` navigates to the review page instead. */
  readonly onClick?: () => void;
  readonly to?: string;
};

/**
 * The pending-review indicator, shared by every surface that reports one.
 *
 * Renders the `awaiting_input` StateBadge so it matches the state a parked task shows,
 * and carries only the icon and count — the full wording lives in the tooltip, which
 * also names the control for assistive tech.
 */
export const NeedsReviewIndicator = ({ count, onClick, to }: Props) => {
  const { i18n, t: translate } = useTranslation("hitl");

  if (count <= 0) {
    return undefined;
  }

  const label = translate("requiredActionCount", { count });
  const badge = (
    <StateBadge colorPalette="awaiting_input" fontSize="md" variant="solid">
      <LuUserRoundPen />
      {formatNumber(count, i18n.language)}
    </StateBadge>
  );

  return (
    <Tooltip content={label}>
      {to === undefined ? (
        <Button aria-label={label} data-testid="needs-review-badge" onClick={onClick} variant="plain">
          {badge}
        </Button>
      ) : (
        <Button aria-label={label} asChild data-testid="needs-review-badge" variant="plain">
          <RouterLink to={to}>{badge}</RouterLink>
        </Button>
      )}
    </Tooltip>
  );
};
