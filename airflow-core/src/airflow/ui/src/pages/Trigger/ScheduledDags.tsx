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
import { HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { TriggerAssetWatcherReferenceResponse } from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import { getScheduledDagIds } from "src/pages/Triggers/getScheduledDagIds";

/** The Dags scheduled on the watched assets, as the trigger lists them: only the Dags the user can read. */
export const ScheduledDags = ({
  watchers,
}: {
  readonly watchers: Array<TriggerAssetWatcherReferenceResponse>;
}) => {
  const { t: translate } = useTranslation("browse");
  const dagIds = getScheduledDagIds(watchers);

  // A watcher only runs while a Dag is scheduled on its asset, so none here means none the user can read.
  return dagIds.length === 0 ? (
    <Text color="fg.muted">{translate("triggers.detail.noReadableScheduledDags")}</Text>
  ) : (
    <HStack flexWrap="wrap" gap={3}>
      {dagIds.map((dagId) => (
        <RouterLink key={dagId} to={`/dags/${dagId}`}>
          {dagId}
        </RouterLink>
      ))}
    </HStack>
  );
};
