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
import { Box, Heading, HStack, Table } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiInfo } from "react-icons/fi";

import type { TaskInstanceRetryDetails } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";
import { renderCompactDuration } from "src/utils/datetimeUtils";

export const RetryDetails = ({ details }: { readonly details: TaskInstanceRetryDetails }) => {
  const { t: translate } = useTranslation();

  return (
    <Box py={1}>
      <Heading py={1} size="sm">
        {translate("taskInstance.retry.title")}
      </Heading>
      <Table.Root striped>
        <Table.Body>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.retry.eligibleAt")}</Table.Cell>
            <Table.Cell>
              <Time datetime={details.eligible_at} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.retry.finalDelay")}</Table.Cell>
            <Table.Cell>
              {renderCompactDuration(details.delay_seconds)}
              {details.is_capped ? ` (${translate("taskInstance.retry.capped")})` : undefined}
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.retry.source")}</Table.Cell>
            <Table.Cell>{translate(`taskInstance.retry.sources.${details.source}`)}</Table.Cell>
          </Table.Row>
          {details.configured_delay_seconds === null ? undefined : (
            <Table.Row>
              <Table.Cell>{translate("taskInstance.retry.configuredDelay")}</Table.Cell>
              <Table.Cell>{renderCompactDuration(details.configured_delay_seconds)}</Table.Cell>
            </Table.Row>
          )}
          {details.backoff_delay_seconds === null ? undefined : (
            <Table.Row>
              <Table.Cell>{translate("taskInstance.retry.backoffDelay")}</Table.Cell>
              <Table.Cell>{renderCompactDuration(details.backoff_delay_seconds)}</Table.Cell>
            </Table.Row>
          )}
          {details.jitter_seconds === null ? undefined : (
            <Table.Row>
              <Table.Cell>
                <HStack gap={1}>
                  {translate("taskInstance.retry.jitter")}
                  <Tooltip content={translate("taskInstance.retry.jitterTooltip")}>
                    <FiInfo size={12} />
                  </Tooltip>
                </HStack>
              </Table.Cell>
              <Table.Cell>{renderCompactDuration(details.jitter_seconds)}</Table.Cell>
            </Table.Row>
          )}
          {details.maximum_delay_seconds === null ? undefined : (
            <Table.Row>
              <Table.Cell>{translate("taskInstance.retry.maximumDelay")}</Table.Cell>
              <Table.Cell>{renderCompactDuration(details.maximum_delay_seconds)}</Table.Cell>
            </Table.Row>
          )}
          {details.reason === null ? undefined : (
            <Table.Row>
              <Table.Cell>{translate("taskInstance.retry.reason")}</Table.Cell>
              <Table.Cell>{details.reason}</Table.Cell>
            </Table.Row>
          )}
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
