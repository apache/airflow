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

import { HStack, Text } from "@chakra-ui/react";
import type { FC } from "react";

import type { TraceListItem } from "src/types";
import { aggregate, fmtCost, fmtLatency } from "src/util";

const Stat: FC<{ label: string; value: string }> = ({ label, value }) => (
  <Text color="fg.muted" fontSize="xs" whiteSpace="nowrap">
    <Text as="span" color="fg" fontWeight="semibold">
      {value}
    </Text>{" "}
    {label}
  </Text>
);

export const Aggregates: FC<{ items: TraceListItem[] }> = ({ items }) => {
  const a = aggregate(items);
  return (
    <HStack divideX="1px" gap={3}>
      <Stat label="traces" value={String(a.count)} />
      <Stat label="p50 lat" value={fmtLatency(a.p50Latency)} />
      <Stat label="p95 lat" value={fmtLatency(a.p95Latency)} />
      <Stat label="cost" value={fmtCost(a.totalCost)} />
    </HStack>
  );
};
