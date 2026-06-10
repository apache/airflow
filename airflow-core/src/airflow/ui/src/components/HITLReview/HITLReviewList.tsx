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
import { useTranslation } from "react-i18next";

import type { HITLDetail } from "openapi/requests/types.gen.ts";
import Time from "src/components/Time.tsx";
import { useTimezone } from "src/context/timezone";

dayjs.extend(utc);
dayjs.extend(tz);

const getHitlReviewListDateFormat = (datetime: string, showSeconds: boolean, timezone: string) => {
  if (dayjs(datetime).tz(timezone).isSame(dayjs().tz(timezone), "day")) {
    return `HH:mm${showSeconds ? ":ss" : ""}`;
  }

  return `MMM D, HH:mm${showSeconds ? ":ss" : ""}`;
};

const TableColumnHeader = ({ children, width }: { readonly children: string; readonly width?: string }) => (
  <Table.ColumnHeader color="fg.muted" fontSize="xs" fontWeight="medium" px={2} py={1.5} w={width}>
    {children}
  </Table.ColumnHeader>
);

const HITL_GROUP_COLORS = ["green.solid", "purple.solid"] as const;

const getHitlGroupColor = (details: Array<HITLDetail>, index: number) => {
  let groupIndex = 0;

  for (let currentIndex = 0; currentIndex <= index; currentIndex += 1) {
    const detail = details[currentIndex];
    const previous = details[currentIndex - 1];

    if (
      detail !== undefined &&
      previous !== undefined &&
      detail.task_instance.dag_id !== previous.task_instance.dag_id
    ) {
      groupIndex += 1;
    }
  }

  return HITL_GROUP_COLORS[groupIndex % HITL_GROUP_COLORS.length] ?? HITL_GROUP_COLORS[0];
};

export const HITLReviewList = ({
  details,
  onSelect,
  selectedKey,
}: {
  readonly details: Array<HITLDetail>;
  readonly onSelect: (selection: HITLDetail) => void;
  readonly selectedKey?: string;
}) => {
  const { t: translate } = useTranslation(["hitl", "common"]);
  const { selectedTimezone } = useTimezone();

  return (
    <Table.Root minW="640px" size="sm" tableLayout="fixed" width="100%">
      <Table.Header>
        <Table.Row>
          <TableColumnHeader width="30%">{translate("common:dagId")}</TableColumnHeader>
          <TableColumnHeader width="76px">{translate("common:dagRun_one")}</TableColumnHeader>
          <TableColumnHeader width="76px">{translate("common:mapIndex")}</TableColumnHeader>
          <TableColumnHeader>{translate("common:taskId")}</TableColumnHeader>
          <TableColumnHeader width="88px">{translate("common:table.createdAt")}</TableColumnHeader>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {details.length === 0
          ? null
          : details.map((detail, index) => {
              const isSelected = selectedKey === detail.task_instance.id;
              const ti = detail.task_instance;

              return (
                <Table.Row
                  _hover={{ bg: isSelected ? "bg.muted" : "bg.subtle" }}
                  aria-pressed={isSelected}
                  bg={isSelected ? "bg.muted" : undefined}
                  cursor="pointer"
                  key={detail.task_instance.id}
                  onClick={() => onSelect(detail)}
                >
                  <Table.Cell
                    borderLeftColor={getHitlGroupColor(details, index)}
                    borderLeftWidth={3}
                    overflow="hidden"
                    px={2}
                    py={1.5}
                  >
                    <Text fontSize="xs" truncate>
                      {ti.dag_id}
                    </Text>
                  </Table.Cell>
                  <Table.Cell px={2} py={1.5}>
                    <Text fontSize="xs">
                      <Time
                        datetime={ti.run_after}
                        format={getHitlReviewListDateFormat(ti.run_after, true, selectedTimezone)}
                      />
                    </Text>
                  </Table.Cell>
                  <Table.Cell px={2} py={1.5}>
                    <Text color="fg.muted" fontSize="xs">
                      {ti.rendered_map_index ?? (ti.map_index === -1 ? undefined : String(ti.map_index))}
                    </Text>
                  </Table.Cell>
                  <Table.Cell overflow="hidden" px={2} py={1.5}>
                    <Text fontSize="xs" truncate>
                      {ti.task_id}
                    </Text>
                  </Table.Cell>
                  <Table.Cell px={2} py={1.5}>
                    <Text fontSize="xs">
                      <Time
                        datetime={detail.created_at}
                        format={getHitlReviewListDateFormat(detail.created_at, false, selectedTimezone)}
                      />
                    </Text>
                  </Table.Cell>
                </Table.Row>
              );
            })}
      </Table.Body>
    </Table.Root>
  );
};
