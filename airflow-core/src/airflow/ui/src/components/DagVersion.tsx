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

import type { DagVersionResponse } from "openapi/requests/types.gen";

import Time from "./Time";
import { Tooltip } from "./ui";

export const DagVersion = ({ version }: { readonly version: DagVersionResponse | null | undefined }) => {
  const { t: translate } = useTranslation("components");

  if (version === null || version === undefined) {
    return undefined;
  }

  return (
    <Tooltip content={<Time datetime={version.created_at} />}>
      <Text as="span">{translate("versionSelect.versionCode", { versionCode: version.version_number })}</Text>
    </Tooltip>
  );
};
