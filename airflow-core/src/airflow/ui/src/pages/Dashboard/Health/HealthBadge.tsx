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
import { Skeleton, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";

export const HealthBadge = ({
  isLoading,
  latestHeartbeat,
  status,
  title,
}: {
  readonly isLoading: boolean;
  readonly latestHeartbeat?: string | null;
  readonly status?: string | null;
  readonly title: string;
}) => {
  const { t: translate } = useTranslation("dashboard");

  if (isLoading) {
    return <Skeleton borderRadius="full" height={8} width={24} />;
  }

  const state = status === "healthy" ? "success" : "failed";

  return (
    <Tooltip
      content={
        <div>
          <Text>
            {translate("health.status")}
            {": "}
            {translate(`health.${status}`)}
          </Text>
          <Text hidden={latestHeartbeat === undefined}>
            {translate("health.lastHeartbeat")}
            {": "}
            <Time datetime={latestHeartbeat} />
          </Text>
        </div>
      }
    >
      <StateBadge size="lg" state={state} variant="surface">
        {title}
      </StateBadge>
    </Tooltip>
  );
};
