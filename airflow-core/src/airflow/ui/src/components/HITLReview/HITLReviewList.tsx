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
import { Table } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { HITLDetail } from "openapi/requests/types.gen.ts";
import Time from "src/components/Time.tsx";

const TableColumnHeader = ({ children, width }: { readonly children: string; readonly width?: string }) => (
  <Table.ColumnHeader w={width}>{children}</Table.ColumnHeader>
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
  selectedDetail,
}: {
  readonly details: Array<HITLDetail>;
  readonly onSelect: (selection: HITLDetail) => void;
  readonly selectedDetail?: HITLDetail;
}) => {
  const { t: translate } = useTranslation(["hitl", "common"]);

  return (
    <Table.Root minW="max-content">
      <Table.Header>
        <Table.Row>
          <TableColumnHeader width="30%">{translate("common:dagId")}</TableColumnHeader>
          <TableColumnHeader width="170px">{translate("common:dagRun_one")}</TableColumnHeader>
          <TableColumnHeader width="90px">{translate("common:mapIndex")}</TableColumnHeader>
          <TableColumnHeader>{translate("common:taskId")}</TableColumnHeader>
          <TableColumnHeader width="170px">{translate("common:table.createdAt")}</TableColumnHeader>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {details.length === 0
          ? null
          : details.map((detail, index) => {
              const isSelected = selectedDetail?.task_instance.id === detail.task_instance.id;
              const ti = detail.task_instance;

              return (
                <Table.Row
                  _hover={{ bg: isSelected ? "bg.muted" : "bg.subtle" }}
                  aria-pressed={isSelected}
                  bg={isSelected ? "bg.muted" : undefined}
                  cursor="pointer"
                  key={detail.task_instance.id}
                  onClick={() => onSelect(detail)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      onSelect(detail);
                    }
                  }}
                  py={2}
                  tabIndex={0}
                >
                  <Table.Cell borderLeftColor={getHitlGroupColor(details, index)} borderLeftWidth={3}>
                    {ti.dag_id}
                  </Table.Cell>
                  <Table.Cell>
                    <Time datetime={ti.run_after} />
                  </Table.Cell>
                  <Table.Cell>
                    {ti.rendered_map_index ?? (ti.map_index === -1 ? undefined : String(ti.map_index))}
                  </Table.Cell>
                  <Table.Cell>{ti.task_id}</Table.Cell>
                  <Table.Cell>
                    <Time datetime={detail.created_at} />
                  </Table.Cell>
                </Table.Row>
              );
            })}
      </Table.Body>
    </Table.Root>
  );
};
