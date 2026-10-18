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
import { Box, Text, HStack } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { FiDatabase } from "react-icons/fi";
import { Link } from "react-router-dom";

import type { AssetEventResponse } from "openapi/requests/types.gen";

import { Tooltip } from "src/system-components";

import Time from "src/components/Time";

import { SearchParamsKeys } from "src/constants/searchParams";
import { WEBHOOK_NAME_PATTERN } from "src/constants/webhooks";

import RenderedJsonField from "../RenderedJsonField";
import { TriggeredRuns } from "./TriggeredRuns";

type EventSource = {
  /** The extra key that named the source, which the rendered extra leaves out. */
  readonly flag?: string;
  readonly label: string;
  readonly path?: string;
};

// A source task wins over every flag: a task can set any of them in its outlet events' extra.
const getEventSource = (event: AssetEventResponse, translate: TFunction): EventSource | undefined => {
  const { extra, source_dag_id: sourceDagId } = event;

  if (typeof sourceDagId === "string" && sourceDagId !== "") {
    return {
      label: sourceDagId,
      path: `/dags/${sourceDagId}/runs/${event.source_run_id}/tasks/${event.source_task_id}${event.source_map_index > -1 ? `/mapped/${event.source_map_index}` : ""}`,
    };
  }
  if (extra?.from_rest_api === true) {
    return { flag: "from_rest_api", label: translate("sources.api") };
  }
  if (extra?.from_trigger === true) {
    return { flag: "from_trigger", label: translate("sources.trigger") };
  }
  if (extra?.from_webhook === true) {
    const { inbound_delivery_id: inboundDeliveryId, webhook } = extra;

    return {
      flag: "from_webhook",
      label: translate("sources.webhook"),
      path:
        typeof webhook === "string" &&
        WEBHOOK_NAME_PATTERN.test(webhook) &&
        typeof inboundDeliveryId === "string"
          ? `/webhook_deliveries/${webhook}?${SearchParamsKeys.ID}=${encodeURIComponent(inboundDeliveryId)}`
          : undefined,
    };
  }

  return undefined;
};

export const AssetEvent = ({
  assetId,
  event,
}: {
  readonly assetId?: number;
  readonly event: AssetEventResponse;
}) => {
  const { t: translate } = useTranslation("dashboard");
  const { t: rootTranslate } = useTranslation();

  const source = getEventSource(event, translate);
  const extra = Object.fromEntries(Object.entries(event.extra ?? {}).filter(([key]) => key !== source?.flag));

  return (
    <Box borderBottomWidth={1} fontSize={13} pb={2}>
      <Text fontWeight="bold">
        <Time datetime={event.timestamp} />
      </Text>
      {Boolean(assetId) ? undefined : (
        <HStack>
          <Box>
            <FiDatabase />
          </Box>
          <Tooltip
            content={
              <div>
                <Text>
                  {translate("group")}: {event.group ?? ""}
                </Text>
                <Text>
                  {translate("uri")}: {event.uri ?? ""}
                </Text>
              </div>
            }
            showArrow
          >
            <Link to={`/assets/${event.asset_id}`}>
              <Box color="fg.info" overflowWrap="anywhere" padding={0}>
                {event.name ?? ""}
              </Box>
            </Link>
          </Tooltip>
        </HStack>
      )}
      {source === undefined ? undefined : (
        <HStack flexWrap="wrap">
          <Box>{translate("source")}: </Box>
          {source.path === undefined ? (
            source.label
          ) : (
            <Link to={source.path}>
              <Box color="fg.info" overflowWrap="anywhere" padding={0}>
                {source.label}
              </Box>
            </Link>
          )}
        </HStack>
      )}
      <HStack>
        <TriggeredRuns dagRuns={event.created_dagruns} />
      </HStack>
      {
        // eslint-disable-next-line no-eq-null, eqeqeq
        event.partition_key == null ? undefined : (
          <HStack>
            <Text>
              {rootTranslate("dagRun.partitionKey")}: {event.partition_key}
            </Text>
          </HStack>
        )
      }
      {Object.keys(extra).length >= 1 ? <RenderedJsonField collapsed content={extra} /> : undefined}
    </Box>
  );
};
