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

import type { ExpandedApiError } from "src/components/ErrorAlert";
import { toaster } from "src/components/ui";

type ErrorToastMessage = {
  readonly description: string;
  readonly title: string;
};

export const createErrorToaster =
  (translate: TFunction, fallbackMessage: ErrorToastMessage) => (error: unknown) => {
    const isForbidden = (error as ExpandedApiError).status === 403;

    toaster.create({
      description: isForbidden
        ? translate("toaster.forbidden.description", { ns: "common" })
        : fallbackMessage.description,
      title: isForbidden ? translate("toaster.forbidden.title", { ns: "common" }) : fallbackMessage.title,
      type: "error",
    });
  };
