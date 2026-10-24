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
import { Link, List, Table, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import type { WebhookMatchResponse } from "openapi/requests/types.gen";

import { formatMatchValue } from "src/utils/webhookMatch";

type WebhookMatchListProps = {
  readonly matches: Array<WebhookMatchResponse>;
};

/** The conditions on webhook deliveries that a Dag's or a task's `cancel_on` declares. */
export const WebhookMatchList = ({ matches }: WebhookMatchListProps) => {
  const { t: translate } = useTranslation("common");

  return (
    <List.Root data-testid="webhook-match-list" gap={2} ps={4}>
      {matches.map((match) => (
        <List.Item data-testid="webhook-match" key={JSON.stringify(match)}>
          <Table.Root size="sm">
            <Table.Body>
              <Table.Row>
                <Table.Cell>{translate("webhookMatch.webhook")}</Table.Cell>
                <Table.Cell>
                  <Link asChild color="fg.info">
                    <RouterLink to={`/webhook_deliveries/${match.webhook}`}>{match.webhook}</RouterLink>
                  </Link>
                </Table.Cell>
              </Table.Row>
              <Table.Row>
                <Table.Cell>{translate("webhookMatch.events")}</Table.Cell>
                <Table.Cell>
                  {match.events === null || match.events === undefined
                    ? translate("webhookMatch.anyEvent")
                    : match.events.join(", ")}
                </Table.Cell>
              </Table.Row>
              <Table.Row>
                <Table.Cell>{translate("webhookMatch.match")}</Table.Cell>
                <Table.Cell>
                  {Object.keys(match.match ?? {}).length === 0 ? (
                    translate("webhookMatch.anyMatch")
                  ) : (
                    <Table.Root data-testid="webhook-match-attributes" size="sm">
                      <Table.Body>
                        {Object.entries(match.match ?? {}).map(([key, values]) => (
                          <Table.Row key={key}>
                            <Table.Cell>{key}</Table.Cell>
                            <Table.Cell>{formatMatchValue(translate, values)}</Table.Cell>
                          </Table.Row>
                        ))}
                      </Table.Body>
                    </Table.Root>
                  )}
                </Table.Cell>
              </Table.Row>
            </Table.Body>
          </Table.Root>
          {match.correlation_key_conf === null || match.correlation_key_conf === undefined ? undefined : (
            <Text color="fg.muted" pt={1}>
              {translate("webhookMatch.correlationKeyConf", { key: match.correlation_key_conf })}
            </Text>
          )}
        </List.Item>
      ))}
    </List.Root>
  );
};
