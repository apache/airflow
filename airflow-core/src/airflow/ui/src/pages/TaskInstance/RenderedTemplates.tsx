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
import { Box, HStack, Table } from "@chakra-ui/react";
import { useParams } from "react-router-dom";
import { PrismLight as SyntaxHighlighter } from "react-syntax-highlighter";
import bash from "react-syntax-highlighter/dist/esm/languages/prism/bash";
import json from "react-syntax-highlighter/dist/esm/languages/prism/json";
import sql from "react-syntax-highlighter/dist/esm/languages/prism/sql";
import yaml from "react-syntax-highlighter/dist/esm/languages/prism/yaml";
import { oneLight, oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";

SyntaxHighlighter.registerLanguage("json", json);
SyntaxHighlighter.registerLanguage("yaml", yaml);
SyntaxHighlighter.registerLanguage("sql", sql);
SyntaxHighlighter.registerLanguage("bash", bash);

const detectLanguage = (value: string): string => {
  const trimmed = value.trim();

  // Try to detect JSON
  if (
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  ) {
    try {
      JSON.parse(trimmed);

      return "json";
    } catch {
      // Not valid JSON, continue
    }
  }

  // Try to detect YAML (basic heuristics)
  if (trimmed.includes(":") && (trimmed.includes("\n") || trimmed.includes("- "))) {
    return "yaml";
  }

  // Try to detect SQL (basic heuristics)
  const sqlKeywords = /\b(?:select|insert|update|delete|create|alter|drop|from|where|join)\b/iu;

  if (sqlKeywords.test(trimmed)) {
    return "sql";
  }

  // Try to detect Bash (basic heuristics)
  const bashKeywords =
    /\b(?:echo|ls|cd|mkdir|rm|cp|mv|grep|awk|sed|cat|chmod|chown|ps|kill|sudo|export|source|if|then|else|fi|for|while|do|done)\b/u;
  const bashCommands = /\$\{|\$\(|&&|\|\||>>|<<|;/u;
  const bashPipe = /(?:^|[^|])\|(?:[^|]|$)/u;

  if (
    trimmed.startsWith("#!") ||
    bashKeywords.test(trimmed) ||
    bashCommands.test(trimmed) ||
    bashPipe.test(trimmed)
  ) {
    return "bash";
  }

  // Default to text (no highlighting)
  return "text";
};

export const RenderedTemplates = () => {
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const { colorMode } = useColorMode();

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance({
    dagId,
    dagRunId: runId,
    mapIndex: parseInt(mapIndex, 10),
    taskId,
  });

  const style = colorMode === "dark" ? oneDark : oneLight;

  return (
    <Box p={2}>
      <Table.Root striped>
        <Table.Body>
          {Object.entries(taskInstance?.rendered_fields ?? {}).map(([key, value]) => {
            if (value !== null && value !== undefined) {
              const renderedValue = typeof value === "string" ? value : JSON.stringify(value);
              const language = detectLanguage(renderedValue);

              return (
                <Table.Row key={key}>
                  <Table.Cell>{key}</Table.Cell>
                  <Table.Cell>
                    <HStack alignItems="flex-start">
                      <Box
                        css={{
                          "& pre": {
                            borderRadius: "4px",
                            fontSize: "14px",
                            margin: 0,
                            padding: "8px",
                          },
                        }}
                        flex="1"
                      >
                        <SyntaxHighlighter language={language} style={style} wrapLongLines={true}>
                          {renderedValue}
                        </SyntaxHighlighter>
                      </Box>
                      <ClipboardRoot value={renderedValue}>
                        <ClipboardIconButton />
                      </ClipboardRoot>
                    </HStack>
                  </Table.Cell>
                </Table.Row>
              );
            }

            return undefined;
          })}
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
