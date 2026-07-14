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

import { Badge, Box, Button, Code, HStack, Input, Link, Spinner, Stack, Table, Text } from "@chakra-ui/react";
import { type FC, useCallback, useEffect, useMemo, useState } from "react";

import { ApiError, fetchTraceList } from "src/api";
import { Aggregates } from "src/components/Aggregates";
import { AgentsView } from "src/components/AgentsView";
import { DEFAULT_FILTERS, Filters, type FilterState } from "src/components/Filters";
import { TraceModal } from "src/components/TraceModal";
import type { TraceListItem } from "src/types";
import { fmtCost, fmtLatency, fmtTimestamp, sinceFromWindow } from "src/util";

function uiBasePath(): string {
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);

  return baseUrl.pathname.replace(/\/$/, "");
}

function taskDetailHref(item: TraceListItem): string {
  const runId = encodeURIComponent(item.run_id);

  return `${uiBasePath()}/dags/${item.dag_id}/runs/${runId}/tasks/${item.task_id}/plugin/ai-trace`;
}

const stateColor: Record<string, string> = {
  failed: "red",
  running: "blue",
  success: "green",
  up_for_retry: "orange",
};

function matchesSearch(item: TraceListItem, needle: string): boolean {
  if (!needle) return true;
  const haystack =
    `${item.dag_id} ${item.task_id} ${item.model ?? ""} ${item.input_preview ?? ""}`.toLowerCase();
  return haystack.includes(needle.toLowerCase());
}

// Deep-linkable selection via ?trace=<id> (query param, not a path segment,
// so opening/closing the pane never remounts the table).
function readTraceParam(): string | null {
  return new URLSearchParams(globalThis.location.search).get("trace");
}

function writeTraceParam(traceId: string | null) {
  const url = new URL(globalThis.location.href);
  if (traceId) url.searchParams.set("trace", traceId);
  else url.searchParams.delete("trace");
  globalThis.history.replaceState(null, "", url.toString());
}

const TraceLookup: FC<{ onOpen: (id: string) => void }> = ({ onOpen }) => {
  const [id, setId] = useState("");

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = id.trim();
    if (trimmed) onOpen(trimmed);
  };

  return (
    <form onSubmit={submit}>
      <HStack gap={2}>
        <Input
          onChange={(e) => setId(e.target.value)}
          placeholder="open trace by id…"
          size="xs"
          value={id}
          width="260px"
        />
        <Button size="xs" type="submit" variant="outline">
          Open
        </Button>
      </HStack>
    </form>
  );
};

type SortKey = "cost" | "latency" | "start_date" | "state" | "total_tokens";

function compareBy(key: SortKey, dir: 1 | -1) {
  return (a: TraceListItem, b: TraceListItem) => {
    const av = a[key];
    const bv = b[key];
    if (av == null && bv == null) return 0;
    if (av == null) return 1; // nulls last regardless of direction
    if (bv == null) return -1;
    if (av < bv) return -dir;
    if (av > bv) return dir;
    return 0;
  };
}

export const TraceList: FC<{ dagId?: string }> = ({ dagId }) => {
  const [items, setItems] = useState<TraceListItem[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<FilterState>(DEFAULT_FILTERS);
  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(readTraceParam);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [sort, setSort] = useState<{ dir: 1 | -1; key: SortKey } | null>(null);
  const [view, setView] = useState<"agents" | "traces">("traces");
  const [agentFilter, setAgentFilter] = useState<{ dagId: string; taskId: string } | null>(null);

  const openTrace = useCallback((traceId: string | null) => {
    setSelectedTraceId(traceId);
    writeTraceParam(traceId);
  }, []);

  const since = useMemo(() => sinceFromWindow(filters.window), [filters.window]);
  const minLatency = filters.minLatency.trim() === "" ? undefined : Number(filters.minLatency);
  const minCost = filters.minCost.trim() === "" ? undefined : Number(filters.minCost);

  useEffect(() => {
    // `cancelled` is flipped by this effect's cleanup, which React runs before
    // the next effect fires. It guards against two races: a slow older request
    // resolving after a newer one (out-of-order overwrite of `items`), and a
    // stale error clobbering fresh results. Clearing `error` up front means a
    // filter change or Refresh always recovers a previously-errored view
    // rather than leaving it stuck on the error box until remount.
    let cancelled = false;
    // Debounce so typing in the min-latency/cost inputs doesn't fire a
    // request per keystroke; window/state chip clicks just eat the 300ms.
    const timer = setTimeout(() => {
      setError(null);
      setItems(null);
      fetchTraceList({
        dagId: dagId ?? agentFilter?.dagId,
        minCost: minCost != null && !Number.isNaN(minCost) ? minCost : undefined,
        minLatency: minLatency != null && !Number.isNaN(minLatency) ? minLatency : undefined,
        since,
        state: filters.state || undefined,
        taskId: agentFilter?.taskId,
      })
        .then((res) => {
          if (!cancelled) setItems(res);
        })
        .catch((err: unknown) => {
          if (cancelled) return;
          setError(err instanceof ApiError ? err.message : err instanceof Error ? err.message : String(err));
        });
    }, 300);
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [dagId, agentFilter, since, filters.state, minLatency, minCost, refreshNonce]);

  const filteredItems = useMemo(() => {
    const matched = (items ?? []).filter((item) => matchesSearch(item, filters.q));
    if (!sort) return matched;
    return [...matched].sort(compareBy(sort.key, sort.dir));
  }, [items, filters.q, sort]);

  const toggleSort = (key: SortKey) =>
    setSort((prev) => (prev?.key === key ? (prev.dir === -1 ? { dir: 1, key } : null) : { dir: -1, key }));

  // Idle sortable headers carry a faint ⇅ so sortability is discoverable
  // without hovering; the active column swaps it for the direction arrow.
  const sortMark = (key: SortKey) => (sort?.key === key ? (sort.dir === -1 ? " ▼" : " ▲") : " ⇅");

  if (error) {
    return (
      <Box bg="bg.subtle" borderRadius="lg" borderWidth="1px" color="fg.muted" fontSize="sm" m={5} p={5}>
        {error}
      </Box>
    );
  }

  const showAgents = !dagId && view === "agents";

  return (
    <Box m={5}>
      <Stack gap={3} mb={3}>
        <Stack direction="row" justify="space-between">
          <HStack gap={3}>
            <Text fontSize="lg" fontWeight="semibold">
              {dagId ? `AI Traces — ${dagId}` : "AI Traces"}
            </Text>
            {!dagId && (
              <HStack gap={1}>
                {(["traces", "agents"] as const).map((v) => (
                  <Button
                    colorPalette={view === v ? "blue" : "gray"}
                    key={v}
                    onClick={() => setView(v)}
                    size="xs"
                    variant={view === v ? "solid" : "outline"}
                  >
                    {v === "traces" ? "Traces" : "Agents"}
                  </Button>
                ))}
              </HStack>
            )}
          </HStack>
          <HStack gap={2}>
            <TraceLookup onOpen={openTrace} />
            <Button
              disabled={items === null}
              onClick={() => setRefreshNonce((n) => n + 1)}
              size="xs"
              variant="outline"
            >
              {items === null ? "Loading…" : "Refresh"}
            </Button>
          </HStack>
        </Stack>
        {!showAgents && (
          <>
            <HStack gap={2} wrap="wrap">
              <Filters onChange={setFilters} value={filters} />
              {agentFilter && (
                <Button
                  colorPalette="blue"
                  onClick={() => setAgentFilter(null)}
                  size="xs"
                  variant="surface"
                >
                  Agent: {agentFilter.dagId}.{agentFilter.taskId} ✕
                </Button>
              )}
            </HStack>
            {/* Aggregates are output, not a control -- own line, off the chip row. */}
            {items !== null && (
              <Stack direction="row" justify="flex-end">
                <Aggregates items={filteredItems} />
              </Stack>
            )}
          </>
        )}
      </Stack>

      {showAgents ? (
        <AgentsView
          onSelect={(agentDagId, agentTaskId) => {
            setAgentFilter({ dagId: agentDagId, taskId: agentTaskId });
            setView("traces");
          }}
        />
      ) : (
      <Box overflowX="auto">
        {items === null ? (
            <Box p={5}>
              <Spinner size="sm" />
            </Box>
          ) : filteredItems.length === 0 ? (
            <Box bg="bg.subtle" borderRadius="lg" borderWidth="1px" color="fg.muted" fontSize="sm" p={5}>
              No agent task instances match these filters. Runs using <Code fontSize="xs">@task.agent</Code>{" "}
              / <Code fontSize="xs">@task.llm</Code> with core tracing on will show up here.
            </Box>
          ) : (
            <Table.Root size="sm" striped>
              <Table.Header>
                <Table.Row>
                  {/* Time leads: recency is the primary scanning axis for a
                      trace list; identity (dag/task/run) follows, then state,
                      content, metrics, action. */}
                  <Table.ColumnHeader cursor="pointer" onClick={() => toggleSort("start_date")}>
                    Started{sortMark("start_date")}
                  </Table.ColumnHeader>
                  {!dagId && <Table.ColumnHeader>Dag</Table.ColumnHeader>}
                  <Table.ColumnHeader>Task</Table.ColumnHeader>
                  <Table.ColumnHeader>Run</Table.ColumnHeader>
                  <Table.ColumnHeader cursor="pointer" onClick={() => toggleSort("state")}>
                    State{sortMark("state")}
                  </Table.ColumnHeader>
                  <Table.ColumnHeader>Input</Table.ColumnHeader>
                  <Table.ColumnHeader>Model</Table.ColumnHeader>
                  <Table.ColumnHeader cursor="pointer" onClick={() => toggleSort("total_tokens")}>
                    Tokens{sortMark("total_tokens")}
                  </Table.ColumnHeader>
                  <Table.ColumnHeader cursor="pointer" onClick={() => toggleSort("cost")}>
                    Cost{sortMark("cost")}
                  </Table.ColumnHeader>
                  <Table.ColumnHeader cursor="pointer" onClick={() => toggleSort("latency")}>
                    Latency{sortMark("latency")}
                  </Table.ColumnHeader>
                  <Table.ColumnHeader>Task instance</Table.ColumnHeader>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {filteredItems.map((item) => (
                  <Table.Row
                    _hover={item.trace_id ? { bg: "bg.subtle", cursor: "pointer" } : undefined}
                    bg={selectedTraceId && item.trace_id === selectedTraceId ? "bg.subtle" : undefined}
                    key={`${item.dag_id}-${item.run_id}-${item.task_id}-${item.map_index}`}
                    onClick={() => item.trace_id && openTrace(item.trace_id)}
                  >
                    <Table.Cell>
                      <Text fontSize="xs" whiteSpace="nowrap">
                        {fmtTimestamp(item.start_date)}
                      </Text>
                    </Table.Cell>
                    {!dagId && <Table.Cell>{item.dag_id}</Table.Cell>}
                    <Table.Cell>{item.task_id}</Table.Cell>
                    <Table.Cell maxW="180px">
                      <Text color="fg.muted" fontSize="xs" title={item.run_id} truncate>
                        {item.run_id}
                      </Text>
                    </Table.Cell>
                    <Table.Cell>
                      {item.state ? (
                        <Badge colorPalette={stateColor[item.state] ?? "gray"}>{item.state}</Badge>
                      ) : (
                        "—"
                      )}
                    </Table.Cell>
                    <Table.Cell maxW="280px">
                      <Text color="fg.muted" fontSize="xs" title={item.input_preview ?? undefined} truncate>
                        {item.input_preview ?? "—"}
                      </Text>
                    </Table.Cell>
                    <Table.Cell>
                      {item.model ? (
                        <Badge colorPalette="purple" variant="outline">
                          {item.model}
                        </Badge>
                      ) : (
                        "—"
                      )}
                    </Table.Cell>
                    <Table.Cell>
                      <Text fontSize="xs" whiteSpace="nowrap">
                        {item.input_tokens != null && item.output_tokens != null
                          ? `↗${item.input_tokens} ↙${item.output_tokens}`
                          : (item.total_tokens ?? "—")}
                      </Text>
                    </Table.Cell>
                    <Table.Cell>
                      <Text fontSize="xs">{fmtCost(item.cost)}</Text>
                    </Table.Cell>
                    <Table.Cell>
                      <Text fontSize="xs">{fmtLatency(item.latency)}</Text>
                    </Table.Cell>
                    <Table.Cell>
                      <Link
                        colorPalette="blue"
                        href={taskDetailHref(item)}
                        onClick={(e) => e.stopPropagation()}
                      >
                        View
                      </Link>
                    </Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table.Root>
          )}
      </Box>
      )}

      <TraceModal onClose={() => openTrace(null)} traceId={selectedTraceId} />
    </Box>
  );
};
