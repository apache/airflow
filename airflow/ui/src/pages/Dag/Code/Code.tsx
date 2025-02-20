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
import { Box, Button, Heading, HStack, Field } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import type { OptionsOrGroups, GroupBase, SingleValue } from "chakra-react-select";
import { AsyncSelect } from "chakra-react-select";
import { useState, useCallback } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { createElement, PrismLight as SyntaxHighlighter } from "react-syntax-highlighter";
import python from "react-syntax-highlighter/dist/esm/languages/prism/python";
import { oneLight, oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";

import {
  useDagServiceGetDagDetails,
  useDagSourceServiceGetDagSource,
  UseDagVersionServiceGetDagVersionsKeyFn,
} from "openapi/queries";
import { DagVersionService } from "openapi/requests/services.gen";
import type { DAGVersionCollectionResponse, DagVersionResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { ProgressBar } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";
import { useConfig } from "src/queries/useConfig";

SyntaxHighlighter.registerLanguage("python", python);

const VERSION_PARAM = "version";

type Option = {
  label: string;
  value: string;
};

export const Code = () => {
  const { dagId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedVersion = searchParams.get(VERSION_PARAM);
  const queryClient = useQueryClient();

  const {
    data: dag,
    error,
    isLoading,
  } = useDagServiceGetDagDetails({
    dagId: dagId ?? "",
  });

  const {
    data: code,
    error: codeError,
    isLoading: isCodeLoading,
  } = useDagSourceServiceGetDagSource({
    dagId: dagId ?? "",
    versionNumber: selectedVersion === null ? undefined : parseInt(selectedVersion, 10),
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

  const loadVersions = (
    _: string,
    callback: (options: OptionsOrGroups<Option, GroupBase<Option>>) => void,
  ): Promise<OptionsOrGroups<Option, GroupBase<Option>>> =>
    queryClient.fetchQuery({
      queryFn: () =>
        DagVersionService.getDagVersions({
          dagId: dagId ?? "",
        }).then((data: DAGVersionCollectionResponse) => {
          const options = data.dag_versions.map((version: DagVersionResponse) => {
            const versionNumber = version.version_number.toString();

            return {
              label: versionNumber,
              value: versionNumber,
            };
          });

          callback(options);

          return options;
        }),
      queryKey: UseDagVersionServiceGetDagVersionsKeyFn({ dagId: dagId ?? "" }),
      staleTime: 0,
    });

  const handleStateChange = useCallback(
    (version: SingleValue<Option>) => {
      if (version) {
        searchParams.set(VERSION_PARAM, version.value);
        setSearchParams(searchParams);
      }
    },
    [searchParams, setSearchParams],
  );

  return (
    <Box>
      <HStack justifyContent="space-between" mt={2}>
        {dag?.last_parsed_time !== undefined && (
          <Heading as="h4" fontSize="14px" size="md">
            Parsed at: <Time datetime={dag.last_parsed_time} />
          </Heading>
        )}
        <HStack>
          <Field.Root>
            <AsyncSelect
              defaultOptions
              filterOption={undefined}
              isSearchable={false}
              loadOptions={loadVersions}
              onChange={handleStateChange}
              placeholder="Dag Version"
              value={selectedVersion === null ? null : { label: selectedVersion, value: selectedVersion }} // null is required https://github.com/JedWatson/react-select/issues/3066
            />
          </Field.Root>

          <Button aria-label={wrap ? "Unwrap" : "Wrap"} bg="bg.panel" onClick={toggleWrap} variant="outline">
            {wrap ? "Unwrap" : "Wrap"}
          </Button>
        </HStack>
      </HStack>
      <ErrorAlert error={error ?? codeError} />
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
          {code?.content ?? ""}
        </SyntaxHighlighter>
      </div>
    </Box>
  );
};
