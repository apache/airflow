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
import { Table, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import tz from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import type { HITLDetail } from "openapi/requests/types.gen.ts";
import Time from "src/components/Time.tsx";
import { useTimezone } from "src/context/timezone";
import { getRelativeTime } from "src/utils/datetimeUtils.ts";

dayjs.extend(utc);
dayjs.extend(tz);

const getHitlReviewDetailDateFormat = (datetime: string, timezone: string) =>
  dayjs(datetime).tz(timezone).isSame(dayjs().tz(timezone), "day") ? "HH:mm:ss" : "ddd, MMM D, HH:mm:ss";

const HITLReviewRow = ({ label, value }: { readonly label: string; readonly value: ReactNode }) => (
  <Table.Row>
    <Table.Cell
      as="th"
      color="fg.subtle"
      fontSize="xs"
      fontWeight="normal"
      px={2}
      py={1.5}
      scope="row"
      w="30%"
    >
      {label}
    </Table.Cell>
    <Table.Cell fontSize="xs" px={2} py={1.5}>
      {value}
    </Table.Cell>
  </Table.Row>
);

export const HITLReviewDetailSummary = ({ detail }: { readonly detail: HITLDetail }) => {
  const { t: translate } = useTranslation(["hitl", "common"]);
  const { selectedTimezone } = useTimezone();
  const ti = detail.task_instance;
  const mappedIndex = ti.rendered_map_index ?? (ti.map_index >= 0 ? ti.map_index : undefined);

  return (
    <Table.Root size="sm" tableLayout="fixed" width="100%">
      <Table.Body>
        <HITLReviewRow label={translate("common:dagId")} value={<Text truncate>{ti.dag_id}</Text>} />
        <HITLReviewRow label={translate("common:dagRunId")} value={<Text>{ti.dag_run_id}</Text>} />
        <HITLReviewRow label={translate("common:mapIndex")} value={<Text>{mappedIndex ?? "-"}</Text>} />
        <HITLReviewRow label={translate("common:taskId")} value={<Text truncate>{ti.task_id}</Text>} />
        <HITLReviewRow
          label={translate("common:table.createdAt")}
          value={
            <Text>
              <Time
                datetime={detail.created_at}
                format={getHitlReviewDetailDateFormat(detail.created_at, selectedTimezone)}
              />
              {` (${getRelativeTime(detail.created_at)})`}
            </Text>
          }
        />
        <HITLReviewRow label={translate("common:tryNumber")} value={<Text>{ti.try_number}</Text>} />
      </Table.Body>
    </Table.Root>
  );
};
