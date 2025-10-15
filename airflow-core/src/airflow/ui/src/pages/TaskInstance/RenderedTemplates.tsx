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
import { Box, Table } from "@chakra-ui/react";
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
import { detectLanguage } from "src/utils/detectLanguage";

SyntaxHighlighter.registerLanguage("json", json);
SyntaxHighlighter.registerLanguage("yaml", yaml);
SyntaxHighlighter.registerLanguage("sql", sql);
SyntaxHighlighter.registerLanguage("bash", bash);

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
                    <Box
                      css={{
                        "&:hover .copy-button": {
                          opacity: 1,
                        },
                      }}
                    >
                      <Box as="pre" borderRadius="md" fontSize="sm" m={0} overflowX="auto" p={2}>
                        <SyntaxHighlighter
                          language={language}
                          PreTag="div" // Prevents double <pre> nesting
                          showLineNumbers
                          style={style}
                          wrapLongLines
                        >
                          {renderedValue}
                        </SyntaxHighlighter>
                      </Box>
                      <ClipboardRoot
                        className="copy-button"
                        float="right"
                        marginTop="-3.5rem"
                        opacity={0}
                        position="sticky"
                        right={4}
                        transition="opacity 0.2s ease-in-out"
                        value={renderedValue}
                      >
                        <ClipboardIconButton />
                      </ClipboardRoot>
                    </Box>
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
