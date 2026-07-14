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

import { Badge, Box, Code, Link, Stack, Text } from "@chakra-ui/react";
import { type FC, type ReactNode, useState } from "react";

import { JsonView } from "src/components/JsonView";
import { ObservationTree } from "src/components/ObservationTree";
import type { AirflowRef, TraceSummary } from "src/types";
import { fmtCost, fmtLatency, fmtTimestamp } from "src/util";

function airflowTaskHref(ref: AirflowRef): string {
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const basePath = new URL(baseHref, globalThis.location.origin).pathname.replace(/\/$/, "");
  return `${basePath}/dags/${ref.dag_id}/runs/${encodeURIComponent(ref.run_id)}/tasks/${ref.task_id}`;
}

const Collapsible: FC<{ children: ReactNode; label: string }> = ({ children, label }) => {
  const [open, setOpen] = useState(false);
  return (
    <Stack gap={1}>
      <Text
        color="fg.muted"
        cursor="pointer"
        fontSize="xs"
        onClick={() => setOpen((o) => !o)}
        userSelect="none"
      >
        {open ? "▾" : "▸"} {label}
      </Text>
      {open && children}
    </Stack>
  );
};

const ContentBlock: FC<{ label: string; text: string }> = ({ label, text }) => (
  <Stack gap={1}>
    <Text color="fg.muted" fontSize="xs">
      {label}
    </Text>
    <Box bg="bg" borderRadius="md" borderWidth="1px" fontSize="sm" p={2} whiteSpace="pre-wrap">
      {text}
    </Box>
  </Stack>
);

const roleColor: Record<string, string> = {
  assistant: "green",
  system: "gray",
  tool: "orange",
  user: "blue",
};

// Role-ordered conversation (the Logfire LLM-panel presentation) -- renders
// INSTEAD of the flattened Prompt/Completion blocks when structured messages
// exist, never alongside them.
const Conversation: FC<{ messages: { content: string; role: string }[] }> = ({ messages }) => (
  <Stack gap={1}>
    <Text color="fg.muted" fontSize="xs">
      Conversation
    </Text>
    <Stack gap={2}>
      {messages.map((m, i) => (
        <Stack bg="bg" borderRadius="md" borderWidth="1px" gap={1} key={i} p={2}>
          <Badge colorPalette={roleColor[m.role] ?? "gray"} variant="surface" width="fit-content">
            {m.role}
          </Badge>
          <Text fontSize="sm" whiteSpace="pre-wrap">
            {m.content}
          </Text>
        </Stack>
      ))}
    </Stack>
  </Stack>
);

// Tool-call detail intentionally lives ONLY in the observation tree below
// (each TOOL node lazy-loads its input/output on expand) -- an inline
// "Tool calls" section here duplicated the same content twice per trace.
export const TraceCard: FC<{ summary: TraceSummary }> = ({ summary }) => (
  <Stack bg="bg.subtle" borderRadius="lg" borderWidth="1px" gap={3} p={5}>
    <Stack direction="row" justify="space-between">
      <Stack align="baseline" direction="row" gap={3}>
        <Text fontWeight="semibold">AI Trace</Text>
        <Code fontSize="xs">{summary.trace_id}</Code>
      </Stack>
      <Stack direction="row" gap={4}>
        {summary.airflow_ref && (
          <Link colorPalette="blue" href={airflowTaskHref(summary.airflow_ref)}>
            View task instance ↗
          </Link>
        )}
        {/* Null in trace-store mode -- there is no external backend to open. */}
        {summary.langfuse_url && (
          <Link colorPalette="blue" href={summary.langfuse_url} rel="noreferrer" target="_blank">
            Open in Langfuse ↗
          </Link>
        )}
      </Stack>
    </Stack>
    <Stack direction="row" gap={4} wrap="wrap">
      {summary.error != null && (
        <Badge colorPalette="red" title={summary.error}>
          ERROR
        </Badge>
      )}
      {summary.model && <Badge colorPalette="purple">{summary.model}</Badge>}
      {summary.total_tokens != null && <Badge colorPalette="blue">{summary.total_tokens} tokens</Badge>}
      <Badge colorPalette="gray">{summary.observation_count} spans</Badge>
      {summary.latency != null && <Badge variant="outline">{fmtLatency(summary.latency)}</Badge>}
      {summary.cost != null && <Badge variant="outline">{fmtCost(summary.cost)}</Badge>}
      {summary.timestamp && (
        <Text color="fg.muted" fontSize="xs">
          {fmtTimestamp(summary.timestamp)}
        </Text>
      )}
    </Stack>
    {summary.error != null && (
      <Text color="fg.error" fontSize="xs">
        {summary.error}
      </Text>
    )}
    {summary.conversation && summary.conversation.length > 0 ? (
      <Conversation messages={summary.conversation} />
    ) : summary.prompt || summary.completion ? (
      <>
        {summary.prompt && <ContentBlock label="Prompt" text={summary.prompt} />}
        {summary.completion && <ContentBlock label="Completion" text={summary.completion} />}
      </>
    ) : (
      <Text color="fg.muted" fontSize="xs">
        No prompt/completion content -- this run either didn't have{" "}
        <Code fontSize="xs">capture_content=True</Code> set, or content capture is off by default.
      </Text>
    )}
    {summary.observations.length > 0 && (
      <Stack gap={1}>
        <Text color="fg.muted" fontSize="xs">
          Observations ({summary.observations.length})
        </Text>
        <ObservationTree observations={summary.observations} />
      </Stack>
    )}
    {summary.metadata != null && (
      <Collapsible label="Metadata">
        <Box bg="bg" borderRadius="md" borderWidth="1px" maxH="300px" overflowY="auto" p={2}>
          <JsonView value={summary.metadata} />
        </Box>
      </Collapsible>
    )}
  </Stack>
);
