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

import type { DAGWarningResponse } from "openapi/requests/types.gen";

import { StatusMessage } from "src/components/StatusMessage";

type Props = {
  readonly warning?: DAGWarningResponse;
};

export const WarningAlert = ({ warning }: Props) => {
  const { t: translate } = useTranslation("components");

  if (warning === undefined) {
    return undefined;
  }

  // The warning's type is its short form, but it arrives as the backend's own lowercase enum value,
  // which has no business being a heading in any language. An unmapped one falls back to the word.
  return (
    <StatusMessage
      body={warning.message}
      heading={translate(`dagWarnings.types.${warning.warning_type}`, {
        defaultValue: translate("dagWarnings.warning", { count: 1 }),
      })}
      severity="warning"
    />
  );
};
