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

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";

import { ClipboardRoot, ClipboardIconButton } from "src/system-components";

import { SqlParserProvider } from "src/components/SqlParserProvider";

import { useColorMode } from "src/context/colorMode";
import { detectLanguage } from "src/utils/detectLanguage";
import { oneDark, oneLight, SyntaxHighlighter } from "src/utils/syntaxHighlighter";

const RenderedTemplatesContent = () => {
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
              const renderedValue =
                typeof value === "string"
                  ? value.replaceAll("\\n", "\n").replaceAll(/\\$/gmu, "")
                  : JSON.stringify(value, null, 2);
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
                      position="relative"
                    >
                      <Box borderRadius="md" fontSize="sm" m={0} overflowX="auto" p={2}>
                        <SyntaxHighlighter
                          // The prism theme hard-codes `white-space: pre` on the code tag, which
                          // silently defeats `wrapLongLines` unless we override it here.
                          codeTagProps={{ style: { whiteSpace: "pre-wrap", wordBreak: "break-word" } }}
                          language={language}
                          lineNumberStyle={{ minWidth: "2.5em" }}
                          // Combining `wrapLongLines` with `showLineNumbers` makes the library lay
                          // each line out as a flex container, so every highlighted token becomes a
                          // block-level flex item and copied text breaks after each one. The hanging
                          // indent keeps wrapped lines clear of the gutter (2.5em number + 1em pad).
                          lineProps={{
                            style: { display: "block", paddingLeft: "3.5em", textIndent: "-3.5em" },
                          }}
                          PreTag="pre"
                          showLineNumbers
                          style={style}
                          wrapLongLines
                        >
                          {renderedValue}
                        </SyntaxHighlighter>
                      </Box>
                      <ClipboardRoot
                        className="copy-button"
                        opacity={0}
                        position="absolute"
                        right={4}
                        top={6}
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

export const RenderedTemplates = () => (
  <SqlParserProvider>
    <RenderedTemplatesContent />
  </SqlParserProvider>
);
