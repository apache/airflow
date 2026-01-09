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
import { Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiCalendar } from "react-icons/fi";

import type { ExpressionType } from "src/components/AssetExpression";
import { Tooltip } from "src/components/ui";

import { AssetSchedule } from "./AssetSchedule";

type Props = {
  readonly assetExpression: ExpressionType | null | undefined;
  readonly dagId: string;
  readonly latestRunAfter?: string;
  readonly timetableDescription?: string | null;
  readonly timetableSummary: string | null;
};

export const Schedule = ({
  assetExpression,
  dagId,
  latestRunAfter,
  timetableDescription,
  timetableSummary,
}: Props) => {
  const { t: translate } = useTranslation("dags");

  return Boolean(timetableSummary) ? (
    Boolean(assetExpression) || timetableSummary === translate("schedule.asset") ? (
      <AssetSchedule
        assetExpression={assetExpression}
        dagId={dagId}
        latestRunAfter={latestRunAfter}
        timetableSummary={timetableSummary}
      />
    ) : (
      <Tooltip content={timetableDescription}>
        <Text fontSize="sm">
          <FiCalendar style={{ display: "inline" }} /> {timetableSummary}
        </Text>
      </Tooltip>
    )
  ) : undefined;
};
