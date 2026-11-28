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
import { Badge } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { RiArrowGoBackFill } from "react-icons/ri";

import { useBackfillServiceListBackfillsUi } from "openapi/queries";

import { RouterLink } from "src/system-components";

import { useAutoRefresh } from "src/utils";

type Props = {
  readonly dagId: string;
};

/** Reports a backfill that has not finished yet, linking to the Dag's Backfills tab. */
export const BackfillInProgressBadge = ({ dagId }: Props) => {
  const { t: translate } = useTranslation("components");
  // Not keyed to the Dag, whose paused state stops auto-refresh: a backfill keeps running
  // on a paused Dag, so the badge has to keep up with it either way.
  const refetchInterval = useAutoRefresh({});

  const { data } = useBackfillServiceListBackfillsUi({ active: true, dagId }, undefined, {
    enabled: dagId !== "",
    // Only poll while something is still running, so an idle Dag page does not tick against
    // /ui/backfills forever.
    refetchInterval: (query) =>
      query.state.data?.backfills.some((backfill) => backfill.completed_at === null)
        ? refetchInterval
        : false,
  });

  // An active backfill may already be finished; only an incomplete one is in progress.
  if (!(data?.backfills ?? []).some((backfill) => backfill.completed_at === null)) {
    return undefined;
  }

  return (
    <RouterLink to={`/dags/${dagId}/backfills`}>
      <Badge colorPalette="info" cursor="pointer" variant="subtle">
        <RiArrowGoBackFill size={12} />
        {translate("banner.backfillInProgress")}
      </Badge>
    </RouterLink>
  );
};
