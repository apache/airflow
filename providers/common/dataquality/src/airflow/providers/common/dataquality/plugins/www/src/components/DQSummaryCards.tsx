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

import { Box, Grid, Text } from "@chakra-ui/react";
import type { FC } from "react";

import type { DQSummaryRecord } from "src/types/dq";

interface DQSummaryCardsProps {
  summary: DQSummaryRecord;
}

const METRICS: ReadonlyArray<{ colorPalette: string; key: keyof DQSummaryRecord; label: string }> = [
  { colorPalette: "blue", key: "score", label: "Score" },
  { colorPalette: "green", key: "passed", label: "Passed" },
  { colorPalette: "yellow", key: "warned", label: "Warned" },
  { colorPalette: "orange", key: "failed", label: "Failed" },
  { colorPalette: "red", key: "errored", label: "Errored" },
];

function formatMetricValue(key: keyof DQSummaryRecord, value: number | null): string {
  if (value === null) return "—";
  return key === "score" ? value.toFixed(2) : String(value);
}

export const DQSummaryCards: FC<DQSummaryCardsProps> = ({ summary }) => (
  <Grid gap={3} mb={4} templateColumns="repeat(5, 1fr)">
    {METRICS.map(({ colorPalette, key, label }) => (
      <Box key={key} bg="colorPalette.subtle" borderRadius="lg" colorPalette={colorPalette} p={3}>
        <Text color="colorPalette.fg" fontSize="xs" mb={1}>
          {label}
        </Text>
        <Text color="colorPalette.fg" fontSize="xl" fontWeight="semibold">
          {formatMetricValue(key, summary[key])}
        </Text>
      </Box>
    ))}
  </Grid>
);
