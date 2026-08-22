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

import { Button, HStack, Input } from "@chakra-ui/react";
import type { FC } from "react";

import type { TimeWindow } from "src/util";

const WINDOWS: { id: TimeWindow; label: string }[] = [
  { id: "1h", label: "1h" },
  { id: "24h", label: "24h" },
  { id: "7d", label: "7d" },
  { id: "all", label: "All" },
];

// State chip labels stay lowercase deliberately: they're the literal
// TaskInstance state values, matching how Airflow renders state badges.
const STATES: { id: string; label: string }[] = [
  { id: "", label: "Any state" },
  { id: "success", label: "success" },
  { id: "failed", label: "failed" },
  { id: "running", label: "running" },
];

export interface FilterState {
  minCost: string;
  minLatency: string;
  q: string;
  state: string;
  window: TimeWindow;
}

// Default to 7d, not 24h: an agent DAG that last ran two days ago would greet
// every fresh visit with "0 traces" (the first-run cliff both UX reviews hit).
export const DEFAULT_FILTERS: FilterState = { minCost: "", minLatency: "", q: "", state: "", window: "7d" };

const Chips: FC<{
  onChange: (id: string) => void;
  options: { id: string; label: string }[];
  value: string;
}> = ({ onChange, options, value }) => (
  <HStack gap={1}>
    {options.map((o) => (
      <Button
        colorPalette={value === o.id ? "blue" : "gray"}
        key={o.id}
        onClick={() => onChange(o.id)}
        size="xs"
        variant={value === o.id ? "solid" : "outline"}
      >
        {o.label}
      </Button>
    ))}
  </HStack>
);

export const Filters: FC<{ onChange: (next: FilterState) => void; value: FilterState }> = ({
  value,
  onChange,
}) => (
  <HStack gap={3} wrap="wrap">
    <Chips
      onChange={(id) => onChange({ ...value, window: id as TimeWindow })}
      options={WINDOWS}
      value={value.window}
    />
    <Chips onChange={(id) => onChange({ ...value, state: id })} options={STATES} value={value.state} />
    <Input
      onChange={(e) => onChange({ ...value, q: e.target.value })}
      placeholder="dag, task, model, or input contains…"
      size="xs"
      value={value.q}
      width="220px"
    />
    <Input
      onChange={(e) => onChange({ ...value, minLatency: e.target.value })}
      placeholder="min latency (s)"
      size="xs"
      value={value.minLatency}
      width="110px"
    />
    <Input
      onChange={(e) => onChange({ ...value, minCost: e.target.value })}
      placeholder="min cost ($)"
      size="xs"
      value={value.minCost}
      width="100px"
    />
  </HStack>
);
