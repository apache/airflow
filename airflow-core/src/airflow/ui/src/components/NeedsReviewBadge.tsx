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
import { useTranslation } from "react-i18next";
import { LuUserRoundPen } from "react-icons/lu";
import { Link as RouterLink } from "react-router-dom";

import type { HITLDetail } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { Tooltip } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";

type Props = {
  readonly dagId: string;
  readonly pendingActions: Array<HITLDetail>;
};

export const NeedsReviewBadge = ({ dagId, pendingActions }: Props) => {
  const { t: translate } = useTranslation("hitl");

  if (pendingActions.length === 0) {
    return undefined;
  }

  return (
    <Tooltip content={translate("requiredActionCount", { count: pendingActions.length })}>
      <RouterLink to={`/dags/${dagId}/required_actions?${SearchParamsKeys.RESPONSE_RECEIVED}=false`}>
        <StateBadge colorPalette="deferred" fontSize="md" variant="solid">
          <LuUserRoundPen />
          {pendingActions.length}
        </StateBadge>
      </RouterLink>
    </Tooltip>
  );
};
