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
import type { TFunction } from "i18next";

import { webhookDeliveryOutcomeOptions } from "src/constants/stateOptions";

const findOutcome = (outcome: string) =>
  webhookDeliveryOutcomeOptions.items.find((item) => item.value === outcome);

export const getOutcomeLabel = (translate: TFunction, outcome: string): string => {
  const option = findOutcome(outcome);

  return option === undefined ? outcome : translate(option.label);
};

export const getOutcomeExplanation = (translate: TFunction, outcome: string): string =>
  translate(findOutcome(outcome)?.explanation ?? "browse:webhookDeliveries.outcomeExplanations.unknown");

const skipReasonExplanations: Record<string, string> = {
  asset_is_topic: "browse:webhookDeliveries.skipReasons.asset_is_topic",
  asset_not_active: "browse:webhookDeliveries.skipReasons.asset_not_active",
  dag_has_team: "browse:webhookDeliveries.skipReasons.dag_has_team",
  dag_paused: "browse:webhookDeliveries.skipReasons.dag_paused",
  no_subscribers: "browse:webhookDeliveries.skipReasons.no_subscribers",
  partition_key_invalid: "browse:webhookDeliveries.skipReasons.partition_key_invalid",
  partition_key_missing: "browse:webhookDeliveries.skipReasons.partition_key_missing",
};

export const getSkipReasonExplanation = (translate: TFunction, reason: string): string =>
  translate(skipReasonExplanations[reason] ?? "browse:webhookDeliveries.skipReasons.unknown");
