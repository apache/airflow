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

import type { DeadlineAlertResponse } from "openapi/requests/types.gen";
import { humanizeSeconds } from "src/utils/datetimeUtils";

// The API sends a null interval when the alert has no fixed number of seconds — a variable
// interval the scheduler only resolves per run, or a stored value it could not read as a
// duration. Naming a length there would be a lie, so the rule only names the reference point.
// `translate` must be bound to the "dag" namespace.
export const translateCompletionRule = (
  translate: TFunction,
  alert: DeadlineAlertResponse | undefined,
): string | undefined => {
  if (alert === undefined) {
    return undefined;
  }

  const reference = translate(`deadlineAlerts.referenceType.${alert.reference_type}`, {
    defaultValue: alert.reference_type,
  });
  const interval = humanizeSeconds(alert.interval);

  return interval === undefined
    ? translate("deadlineAlerts.completionRuleDynamic", { reference })
    : translate("deadlineAlerts.completionRule", { interval, reference });
};
