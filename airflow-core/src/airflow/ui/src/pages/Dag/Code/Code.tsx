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
import { Box, Button, Heading, HStack, Link } from "@chakra-ui/react";
import { useState } from "react";
import { useParams } from "react-router-dom";
import { createElement, PrismLight as SyntaxHighlighter } from "react-syntax-highlighter";
import python from "react-syntax-highlighter/dist/esm/languages/prism/python";
import { oneLight, oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";

import {
  useDagServiceGetDagDetails,
  useDagSourceServiceGetDagSource,
  useDagVersionServiceGetDagVersion,
} from "openapi/queries";
import type { ApiError } from "openapi/requests/core/ApiError";
import type { DAGSourceResponse } from "openapi/requests/types.gen";
import { DagVersionSelect } from "src/components/DagVersionSelect";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { ClipboardRoot, ClipboardButton } from "src/components/ui";
import { ProgressBar } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { useConfig } from "src/queries/useConfig";

SyntaxHighlighter.registerLanguage("python", python);

export const Code = () => {
  const { dagId } = useParams();

  const selectedVersion = useSelectedVersion();

  const {
    data: dag,
    error,
    isLoading,
  } = useDagServiceGetDagDetails({
    dagId: dagId ?? "",
  });

  const { data: dagVersion } = useDagVersionServiceGetDagVersion(
    {
      dagId: dagId ?? "",
      versionNumber: selectedVersion ?? 1,
    },
    undefined,
    { enabled: dag !== undefined && selectedVersion !== undefined },
  );

  const {
    data: code,
    error: codeError,
    isLoading: isCodeLoading,
  } = useDagSourceServiceGetDagSource<DAGSourceResponse, ApiError | null>({
    dagId: dagId ?? "",
    versionNumber: selectedVersion,
  });

  const defaultWrap = Boolean(useConfig("default_wrap"));

  const [wrap, setWrap] = useState(defaultWrap);

  const toggleWrap = () => setWrap(!wrap);
  const { colorMode } = useColorMode();

  const style = colorMode === "dark" ? oneDark : oneLight;

  // wrapLongLines wasn't working with the prsim styles so we have to manually apply the style
  if (style['code[class*="language-"]'] !== undefined) {
    style['code[class*="language-"]'].whiteSpace = wrap ? "pre-wrap" : "pre";
  }

  return (
    <Box>
      <HStack justifyContent="space-between" mt={2}>
        <HStack gap={5}>
          {dag?.last_parsed_time !== undefined && (
            <Heading as="h4" fontSize="14px" size="md">
              Parsed at: <Time datetime={dag.last_parsed_time} />
            </Heading>
          )}

          {
            // eslint-disable-next-line @typescript-eslint/prefer-optional-chain
            dagVersion !== undefined && dagVersion.bundle_version !== null ? (
              <Heading as="h4" fontSize="14px" size="md" wordBreak="break-word">
                Bundle Version:{" "}
                {dagVersion.bundle_url === null ? (
                  dagVersion.bundle_version
                ) : (
                  <Link
                    aria-label="Bundle Url"
                    color="fg.info"
                    href={dagVersion.bundle_url}
                    rel="noopener noreferrer"
                    target="_blank"
                  >
                    {dagVersion.bundle_version}
                  </Link>
                )}
              </Heading>
            ) : undefined
          }
        </HStack>
        <HStack>
          <DagVersionSelect showLabel={false} />
          <ClipboardRoot value={code?.content ?? ""}>
            <ClipboardButton />
          </ClipboardRoot>
          <Button aria-label={wrap ? "Unwrap" : "Wrap"} bg="bg.panel" onClick={toggleWrap} variant="outline">
            {wrap ? "Unwrap" : "Wrap"}
          </Button>
        </HStack>
      </HStack>
      {/* We want to show an empty state on 404 instead of an error */}
      <ErrorAlert error={error ?? (codeError?.status === 404 ? undefined : codeError)} />
      <ProgressBar size="xs" visibility={isLoading || isCodeLoading ? "visible" : "hidden"} />
      <div
        style={{
          fontSize: "14px",
        }}
      >
        <SyntaxHighlighter
          language="python"
          renderer={({ rows, stylesheet, useInlineStyles }) =>
            rows.map((row, index) => {
              const { children } = row;
              const lineNumberElement = children?.shift();

              // Skip line number span when applying line break styles https://github.com/react-syntax-highlighter/react-syntax-highlighter/issues/376#issuecomment-1584440759
              if (lineNumberElement) {
                if (lineNumberElement.properties) {
                  lineNumberElement.properties.style = {
                    ...(lineNumberElement.properties.style as Record<string, string>),
                    WebkitUserSelect: "none",
                  };
                }

                row.children = [
                  lineNumberElement,
                  {
                    children,
                    properties: {
                      className: [],
                    },
                    tagName: "span",
                    type: "element",
                  },
                ];
              }

              return createElement({
                key: index,
                node: row,
                stylesheet,
                useInlineStyles,
              });
            })
          }
          showLineNumbers
          style={style}
          wrapLongLines={wrap}
        >
          {codeError?.status === 404 && !Boolean(code?.content) ? "No Code Found" : (code?.content ?? "")}
        </SyntaxHighlighter>
      </div>
    </Box>
  );
};
