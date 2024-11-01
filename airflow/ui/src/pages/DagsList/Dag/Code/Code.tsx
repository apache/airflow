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
import { Box, Button, Heading, HStack } from "@chakra-ui/react";
import { useState } from "react";
import { useParams } from "react-router-dom";
import { PrismLight as SyntaxHighlighter } from "react-syntax-highlighter";
import python from "react-syntax-highlighter/dist/esm/languages/prism/python";
import {
  oneLight,
  oneDark,
} from "react-syntax-highlighter/dist/esm/styles/prism";

import {
  useDagServiceGetDagDetails,
  useDagSourceServiceGetDagSource,
} from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { ProgressBar } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";

SyntaxHighlighter.registerLanguage("python", python);

export const Code = () => {
  const { dagId } = useParams();

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
  } = useDagSourceServiceGetDagSource(
    {
      fileToken: dag?.file_token ?? "",
    },
    undefined,
    { enabled: Boolean(dag?.file_token) },
  );

  // TODO: get default_wrap from config
  const [wrap, setWrap] = useState(false);

  const toggleWrap = () => setWrap(!wrap);
  const { colorMode } = useColorMode();

  const style = colorMode === "dark" ? oneDark : oneLight;

  // wrapLongLines wasn't working with the prsim styles so we have to manually apply the style
  if (style['code[class*="language-"]'] !== undefined) {
    style['code[class*="language-"]'].whiteSpace = wrap ? "pre-wrap" : "pre";
  }

  return (
    <Box>
      <ErrorAlert error={error ?? codeError} />
      <ProgressBar
        size="xs"
        visibility={isLoading || isCodeLoading ? "visible" : "hidden"}
      />
      <HStack justifyContent="space-between" my={2}>
        {dag?.last_parsed_time !== undefined && (
          <Heading as="h4" fontSize="14px" pb="10px" size="md">
            Parsed at: <Time datetime={dag.last_parsed_time} />
          </Heading>
        )}
        <Button aria-label="Toggle Wrap" onClick={toggleWrap} variant="outline">
          Toggle Wrap
        </Button>
      </HStack>
      <div
        style={{
          fontSize: "14px",
        }}
      >
        <SyntaxHighlighter
          language="python"
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
