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

import { Badge, Box, Stack, Text } from "@chakra-ui/react";
import { type FC, useEffect, useMemo, useState } from "react";

import { fetchObservationIO } from "src/api";
import { JsonView } from "src/components/JsonView";
import type { ObservationIO, ObservationNode } from "src/types";
import { fmtCost, fmtLatency } from "src/util";

const typeColor: Record<string, string> = {
  GENERATION: "purple",
  SPAN: "gray",
  TOOL: "green",
};

const IOBlock: FC<{ label: string; value: unknown }> = ({ label, value }) => (
  <Stack gap={1}>
    <Text color="fg.muted" fontSize="xs" textTransform="uppercase">
      {label}
    </Text>
    <Box bg="bg" borderRadius="md" borderWidth="1px" maxH="300px" overflowY="auto" p={2}>
      <JsonView value={value} />
    </Box>
  </Stack>
);

interface TimeBounds {
  span: number;
  start: number;
}

// Proportional duration bar anchored to trace start -- the "where did the
// time go" view. Both UX reviews independently ranked this the single most
// important addition: a 32s trace whose spans sum to 5s is invisible without
// it. Pure client-side; start_time/latency are already in the payload.
const WaterfallBar: FC<{ bounds: TimeBounds; node: ObservationNode }> = ({ bounds, node }) => {
  const start = node.start_time ? Date.parse(node.start_time) : NaN;
  if (!bounds.span || Number.isNaN(start)) return null;
  const leftPct = Math.min(100, Math.max(0, ((start - bounds.start) / bounds.span) * 100));
  const widthPct = Math.max(0.8, Math.min(100 - leftPct, (((node.latency ?? 0) * 1000) / bounds.span) * 100));
  const color =
    node.type === "GENERATION" ? "purple.solid" : node.type === "TOOL" ? "green.solid" : "gray.solid";
  return (
    <Box
      alignSelf="center"
      bg="bg.muted"
      borderRadius="sm"
      flexShrink={0}
      height="8px"
      ml="auto"
      position="relative"
      title={`${fmtLatency(node.latency)} from +${((start - bounds.start) / 1000).toFixed(2)}s`}
      width="140px"
    >
      <Box
        bg={color}
        borderRadius="sm"
        height="100%"
        left={`${leftPct}%`}
        position="absolute"
        width={`${widthPct}%`}
      />
    </Box>
  );
};

const TreeNode: FC<{
  bounds: TimeBounds;
  depth: number;
  node: ObservationNode;
  subtree: Map<string, { in: number; out: number }>;
  tree: Map<string | null, ObservationNode[]>;
}> = ({ bounds, depth, node, subtree, tree }) => {
  // All nodes start collapsed: the tree structure + waterfall is the
  // overview, node detail (input/output) is opt-in. Auto-expanding the
  // GENERATION root made its IO read as a duplicate of the card's
  // Prompt/Completion summary blocks.
  const [opened, setOpened] = useState(false);
  const [io, setIo] = useState<ObservationIO | null>(null);
  const [ioLoading, setIoLoading] = useState(false);

  // Trace-store mode inlines IO on the node itself (key present, possibly
  // null); Langfuse mode omits the keys entirely. Key-presence, not
  // truthiness, is the mode signal -- a store SPAN node with null IO must not
  // fall through to a lazy fetch that can only 404.
  const inlineIO = node.input !== undefined || node.output !== undefined;

  // Lazy-fetch this node's input/output the first time it's expanded; the
  // trace-detail payload deliberately strips IO so unopened nodes cost nothing.
  useEffect(() => {
    if (!opened || inlineIO || io !== null || ioLoading) return;
    setIoLoading(true);
    fetchObservationIO(node.id)
      .then(setIo)
      .catch(() => setIo({ id: node.id, input: null, model_parameters: null, output: null }))
      .finally(() => setIoLoading(false));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [opened]);

  const ioInput = inlineIO ? node.input : io?.input;
  const ioOutput = inlineIO ? node.output : io?.output;

  const children = tree.get(node.id) ?? [];
  const isError = node.level === "ERROR" || Boolean(node.status_message);
  const ownInput = node.input_tokens ?? 0;
  const ownOutput = node.output_tokens ?? 0;
  const rolled = subtree.get(node.id);
  // Logfire convention: a parent without its own usage shows the SUM of its
  // descendants' tokens, marked with ∑ to distinguish rollup from direct usage.
  const showRollup = ownInput === 0 && ownOutput === 0 && rolled != null && (rolled.in > 0 || rolled.out > 0);

  return (
    <Box>
      <Stack
        _hover={{ bg: "bg.subtle" }}
        borderRadius="sm"
        cursor="pointer"
        direction="row"
        gap={2}
        onClick={() => setOpened((v) => !v)}
        px={1}
        py={0.5}
      >
        <Text color="fg.muted" fontSize="xs" width="10px">
          {opened ? "▾" : "▸"}
        </Text>
        <Badge colorPalette={typeColor[node.type ?? ""] ?? "gray"} size="sm" variant="surface">
          {node.type ?? "?"}
        </Badge>
        <Text fontSize="sm" fontWeight="medium">
          {node.name ?? node.id}
        </Text>
        <Text color="fg.muted" fontSize="xs">
          {fmtLatency(node.latency)}
          {node.cost != null ? ` · ${fmtCost(node.cost)}` : ""}
          {ownInput > 0 || ownOutput > 0
            ? ` · ↗${ownInput} ↙${ownOutput}`
            : showRollup
              ? ` · ∑ ↗${rolled.in} ↙${rolled.out}`
              : node.total_tokens
                ? ` · ${node.total_tokens} tok`
                : ""}
          {node.model ? ` · ${node.model}` : ""}
        </Text>
        {isError && <Badge colorPalette="red">{node.level ?? "ERROR"}</Badge>}
        <WaterfallBar bounds={bounds} node={node} />
      </Stack>
      {opened && (
        <Stack gap={2} pl={5} py={1}>
          {node.status_message && (
            <Text color="fg.error" fontSize="xs">
              {node.status_message}
            </Text>
          )}
          {ioLoading && (
            <Text color="fg.muted" fontSize="xs">
              Loading input/output…
            </Text>
          )}
          {io?.model_parameters && Object.keys(io.model_parameters).length > 0 && (
            <IOBlock label="model parameters" value={io.model_parameters} />
          )}
          {ioInput != null && <IOBlock label="input" value={ioInput} />}
          {ioOutput != null && <IOBlock label="output" value={ioOutput} />}
        </Stack>
      )}
      {/* Children render unconditionally: the toggle controls THIS node's
          detail, never the tree's shape -- collapsing a parent must not hide
          the structure below it. The rail (borderLeft) lives on the GROUP so
          it draws one continuous line per nesting level; per-node borders
          rendered as broken dangling segments. */}
      {children.length > 0 && (
        <Box borderColor="border.muted" borderLeftWidth="1px" ml={2} pl={4}>
          {children.map((c) => (
            <TreeNode bounds={bounds} depth={depth + 1} key={c.id} node={c} subtree={subtree} tree={tree} />
          ))}
        </Box>
      )}
    </Box>
  );
};

export const ObservationTree: FC<{ observations: ObservationNode[] }> = ({ observations }) => {
  const tree = useMemo(() => {
    const ids = new Set(observations.map((o) => o.id));
    const byParent = new Map<string | null, ObservationNode[]>();
    for (const o of observations) {
      // Langfuse returns "" (not null) for root observations, and a node's
      // parent may not be in the fetched set at all (e.g. a span outside the
      // trace's own observations) -- both must render as roots or they'd
      // silently vanish from the tree.
      const parent = o.parent_observation_id;
      const k = parent && ids.has(parent) ? parent : null;
      const arr = byParent.get(k) ?? [];
      arr.push(o);
      byParent.set(k, arr);
    }
    for (const arr of byParent.values()) {
      arr.sort((a, b) => (a.start_time ?? "").localeCompare(b.start_time ?? ""));
    }
    return byParent;
  }, [observations]);

  const bounds = useMemo<TimeBounds>(() => {
    const starts = observations
      .map((o) => (o.start_time ? Date.parse(o.start_time) : NaN))
      .filter((v) => !Number.isNaN(v));
    if (starts.length === 0) return { span: 0, start: 0 };
    const start = Math.min(...starts);
    const end = Math.max(
      ...observations.map((o) => {
        const s = o.start_time ? Date.parse(o.start_time) : NaN;
        return Number.isNaN(s) ? Number.NEGATIVE_INFINITY : s + (o.latency ?? 0) * 1000;
      }),
    );
    return { span: Math.max(end - start, 1), start };
  }, [observations]);

  // Per-node descendant token sums for the ∑ rollup on parent rows.
  const subtree = useMemo(() => {
    const map = new Map<string, { in: number; out: number }>();
    const compute = (n: ObservationNode): { in: number; out: number } => {
      const acc = { in: n.input_tokens ?? 0, out: n.output_tokens ?? 0 };
      for (const c of tree.get(n.id) ?? []) {
        const s = compute(c);
        acc.in += s.in;
        acc.out += s.out;
      }
      map.set(n.id, acc);
      return acc;
    };
    for (const r of tree.get(null) ?? []) compute(r);
    return map;
  }, [tree]);

  const roots = tree.get(null) ?? [];
  return (
    <Stack gap={1}>
      {roots.map((o) => (
        <TreeNode bounds={bounds} depth={0} key={o.id} node={o} subtree={subtree} tree={tree} />
      ))}
    </Stack>
  );
};
