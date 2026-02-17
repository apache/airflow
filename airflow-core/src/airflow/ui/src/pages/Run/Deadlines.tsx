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
import { Badge, Box, Skeleton, Table, Text } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useDeadlinesServiceGetDagRunDeadlines } from "openapi/queries";
import type { DeadlineResponse } from "openapi/requests";
import Time from "src/components/Time";

type SortField = "alert_name" | "created_at" | "deadline_time" | "missed";
type SortDir = "asc" | "desc";

const sortDeadlines = (deadlines: Array<DeadlineResponse>, field: SortField, dir: SortDir) =>
  [...deadlines].sort((left, right) => {
    let comparison: number;

    if (field === "deadline_time" || field === "created_at") {
      comparison = new Date(left[field]).getTime() - new Date(right[field]).getTime();
    } else if (field === "missed") {
      comparison = Number(left.missed) - Number(right.missed);
    } else {
      const leftName = left.alert_name ?? "";
      const rightName = right.alert_name ?? "";

      comparison = leftName.localeCompare(rightName);
    }

    return dir === "desc" ? -comparison : comparison;
  });

export const Deadlines = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "", runId = "" } = useParams();
  const [sortField, setSortField] = useState<SortField>("deadline_time");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [showMissedOnly, setShowMissedOnly] = useState(false);

  const { data: deadlines, isLoading } = useDeadlinesServiceGetDagRunDeadlines({ dagId, runId });

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDir("asc");
    }
  };

  if (isLoading) {
    return <Skeleton height="200px" />;
  }

  if (deadlines === undefined || deadlines.length === 0) {
    return (
      <Box p={4}>
        <Text color="fg.muted">{translate("deadlines.noDeadlines")}</Text>
      </Box>
    );
  }

  const filteredDeadlines = showMissedOnly ? deadlines.filter((deadline) => deadline.missed) : deadlines;

  const sortedDeadlines = sortDeadlines(filteredDeadlines, sortField, sortDir);

  const sortIndicator = (field: SortField) =>
    sortField === field ? (sortDir === "asc" ? " \u25B2" : " \u25BC") : "";

  return (
    <Box>
      <Box mb={3} p={2}>
        <input
          aria-label={translate("deadlines.showMissedOnly")}
          checked={showMissedOnly}
          onChange={(event) => setShowMissedOnly(event.target.checked)}
          style={{ marginRight: "8px" }}
          type="checkbox"
        />
        {translate("deadlines.showMissedOnly")}
      </Box>

      <Table.Root striped>
        <Table.Header>
          <Table.Row>
            <Table.ColumnHeader cursor="pointer" onClick={() => handleSort("alert_name")}>
              {translate("deadlines.name")}
              {sortIndicator("alert_name")}
            </Table.ColumnHeader>

            <Table.ColumnHeader cursor="pointer" onClick={() => handleSort("deadline_time")}>
              {translate("deadlines.deadlineTime")}
              {sortIndicator("deadline_time")}
            </Table.ColumnHeader>

            <Table.ColumnHeader cursor="pointer" onClick={() => handleSort("missed")}>
              {translate("deadlines.status")}
              {sortIndicator("missed")}
            </Table.ColumnHeader>

            <Table.ColumnHeader cursor="pointer" onClick={() => handleSort("created_at")}>
              {translate("deadlines.createdAt")}
              {sortIndicator("created_at")}
            </Table.ColumnHeader>

            <Table.ColumnHeader>{translate("deadlines.description")}</Table.ColumnHeader>
          </Table.Row>
        </Table.Header>

        <Table.Body>
          {sortedDeadlines.map((deadline) => (
            <Table.Row key={String(deadline.id)}>
              <Table.Cell>{deadline.alert_name}</Table.Cell>

              <Table.Cell>
                <Time datetime={deadline.deadline_time} />
              </Table.Cell>

              <Table.Cell>
                <Badge colorPalette={deadline.missed ? "red" : "green"}>
                  {deadline.missed ? translate("deadlines.missed") : translate("deadlines.onTrack")}
                </Badge>
              </Table.Cell>

              <Table.Cell>
                <Time datetime={deadline.created_at} />
              </Table.Cell>

              <Table.Cell>{deadline.alert_description}</Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
