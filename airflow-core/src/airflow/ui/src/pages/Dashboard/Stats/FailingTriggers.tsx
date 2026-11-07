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

import { useTriggerServiceGetTriggers } from "openapi/queries";

import { StatsCard } from "src/components/StatsCard";

import { useAutoRefresh } from "src/utils";

/** Failing triggers; hidden when there are none, or none the user can see (a 403). */
export const FailingTriggers = () => {
  const { i18n, t: translate } = useTranslation("dashboard");
  const refetchInterval = useAutoRefresh({});
  const { data } = useTriggerServiceGetTriggers({ limit: 1, status: ["failing"] }, undefined, {
    refetchInterval: ({ state }) => state.status !== "error" && refetchInterval,
  });
  const count = data?.total_entries ?? 0;

  return count > 0 ? (
    <StatsCard
      colorScheme="failed"
      count={count}
      isRTL={i18n.dir() === "rtl"}
      label={translate("stats.failingTriggers")}
      link="/triggers?status=failing"
      state="failed"
    />
  ) : undefined;
};
