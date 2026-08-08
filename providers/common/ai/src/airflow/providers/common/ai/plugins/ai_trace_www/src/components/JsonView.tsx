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

import { Box, Button, Text } from "@chakra-ui/react";
import { type FC, useState } from "react";

// Typed, collapsible JSON tree (the Logfire-style "{ 5 items }" viewer) --
// replaces flat pretty-printed strings everywhere structured data renders.

function valueColor(v: unknown): string {
  if (typeof v === "string") return "green.fg";
  if (typeof v === "number") return "blue.fg";
  if (typeof v === "boolean") return "purple.fg";
  return "fg.muted";
}

const JsonNode: FC<{ depth: number; k?: string; v: unknown }> = ({ depth, k, v }) => {
  const [open, setOpen] = useState(depth < 2);

  if (v === null || typeof v !== "object") {
    return (
      <Box>
        {k !== undefined && (
          <Text as="span" color="fg.muted">
            {JSON.stringify(k)}:{" "}
          </Text>
        )}
        <Text as="span" color={valueColor(v)}>
          {JSON.stringify(v)}
        </Text>
      </Box>
    );
  }

  const isArr = Array.isArray(v);
  const entries: [string, unknown][] = isArr
    ? (v as unknown[]).map((x, i) => [String(i), x])
    : Object.entries(v as Record<string, unknown>);
  const [openBracket, closeBracket] = isArr ? ["[", "]"] : ["{", "}"];

  if (entries.length === 0) {
    return (
      <Box>
        {k !== undefined && (
          <Text as="span" color="fg.muted">
            {JSON.stringify(k)}:{" "}
          </Text>
        )}
        <Text as="span">
          {openBracket}
          {closeBracket}
        </Text>
      </Box>
    );
  }

  return (
    <Box>
      <Box cursor="pointer" onClick={() => setOpen((o) => !o)} userSelect="none">
        <Text as="span" color="fg.muted">
          {open ? "▾ " : "▸ "}
        </Text>
        {k !== undefined && (
          <Text as="span" color="fg.muted">
            {JSON.stringify(k)}:{" "}
          </Text>
        )}
        <Text as="span">{openBracket}</Text>
        {!open && (
          <Text as="span" color="fg.muted" fontStyle="italic">
            {" "}
            {entries.length} {isArr ? "items" : "keys"}{" "}
          </Text>
        )}
        {!open && <Text as="span">{closeBracket}</Text>}
      </Box>
      {open && (
        <>
          <Box borderColor="border.muted" borderLeftWidth="1px" pl={4}>
            {entries.map(([ck, cv]) => (
              <JsonNode depth={depth + 1} k={isArr ? undefined : ck} key={ck} v={cv} />
            ))}
          </Box>
          <Text as="span">{closeBracket}</Text>
        </>
      )}
    </Box>
  );
};

export const JsonView: FC<{ value: unknown }> = ({ value }) => {
  const [copied, setCopied] = useState(false);

  // Plain strings render as text, not as a quoted JSON scalar.
  if (typeof value === "string") {
    return (
      <Box fontFamily="mono" fontSize="xs" whiteSpace="pre-wrap">
        {value}
      </Box>
    );
  }

  // A copy button on `{}` or a two-key object just overlaps the content;
  // only offer it when there's enough payload for copying to beat retyping.
  const copyWorthwhile = JSON.stringify(value).length > 60;

  return (
    <Box fontFamily="mono" fontSize="xs" position="relative">
      {copyWorthwhile && (
        <Button
          onClick={() => {
            void navigator.clipboard.writeText(JSON.stringify(value, null, 2));
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
          }}
          position="absolute"
          right={0}
          size="2xs"
          top={0}
          variant="ghost"
          zIndex={1}
        >
          {copied ? "Copied" : "Copy"}
        </Button>
      )}
      <JsonNode depth={0} v={value} />
    </Box>
  );
};
