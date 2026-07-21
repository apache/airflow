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
import { Box, Table, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useBackfillServiceGetBackfill } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";
import { reprocessBehaviors } from "src/constants/reprocessBehaviourParams";

export const Details = () => {
  const { t: translate } = useTranslation();
  const { backfillId = "" } = useParams();

  const { data: backfill, error } = useBackfillServiceGetBackfill({
    backfillId: Number(backfillId),
  });

  if (error !== null && error !== undefined) {
    return <ErrorAlert error={error} />;
  }

  if (backfill === undefined) {
    return undefined;
  }

  const reprocessBehavior = reprocessBehaviors.find(({ value }) => value === backfill.reprocess_behavior);

  return (
    <Box maxWidth="800px" p={4}>
      <Table.Root size="sm" variant="outline">
        <Table.Body>
          <Table.Row>
            <Table.Cell fontWeight="bold">{translate("dagId")}</Table.Cell>
            <Table.Cell>{backfill.dag_id}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell fontWeight="bold">{translate("table.from")}</Table.Cell>
            <Table.Cell>
              <Time datetime={backfill.from_date} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell fontWeight="bold">{translate("table.to")}</Table.Cell>
            <Table.Cell>
              <Time datetime={backfill.to_date} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell fontWeight="bold">{translate("components:backfill.reprocessBehavior")}</Table.Cell>
            <Table.Cell>
              {reprocessBehavior === undefined
                ? backfill.reprocess_behavior
                : translate(`components:${reprocessBehavior.label}`)}
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell fontWeight="bold">{translate("table.maxActiveRuns")}</Table.Cell>
            <Table.Cell>{backfill.max_active_runs}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell fontWeight="bold">{translate("table.createdAt")}</Table.Cell>
            <Table.Cell>
              <Time datetime={backfill.created_at} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell fontWeight="bold">{translate("table.completedAt")}</Table.Cell>
            <Table.Cell>
              {backfill.completed_at === null ? (
                <Text color="fg.muted">—</Text>
              ) : (
                <Time datetime={backfill.completed_at} />
              )}
            </Table.Cell>
          </Table.Row>
          {backfill.dag_run_conf === null ? undefined : (
            <Table.Row>
              <Table.Cell fontWeight="bold">{translate("dagRun.conf")}</Table.Cell>
              <Table.Cell>
                <RenderedJsonField content={backfill.dag_run_conf} />
              </Table.Cell>
            </Table.Row>
          )}
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
