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

import { PrismLight as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneLight } from "react-syntax-highlighter/dist/esm/styles/prism";
import python from "react-syntax-highlighter/dist/esm/languages/prism/python";
import React, { useState } from "react";
import { Box, Button } from "@chakra-ui/react";
import { getMetaValue } from "src/utils";

SyntaxHighlighter.registerLanguage("python", python);

interface Props {
  code: string;
}
export default function CodeBlock({ code }: Props) {
  const [codeWrap, setCodeWrap] = useState(
    getMetaValue("default_wrap") === "True"
  );
  const toggleCodeWrap = () => setCodeWrap(!codeWrap);

  return (
    <Box
      height="calc(100% - 10px)"
      borderWidth={2}
      borderColor="gray:100"
      position="relative"
      margin="0px"
      fontSize="13.5px"
    >
      <Button
        colorScheme="cyan"
        variant="outline"
        background="white"
        aria-label="Toggle Wrap"
        position="absolute"
        top="15px"
        right="30px"
        fontSize="13.5px"
        onClick={toggleCodeWrap}
      >
        Toggle Wrap
      </Button>
      <SyntaxHighlighter
        language="python"
        style={oneLight}
        showLineNumbers
        customStyle={{
          height: "100%",
          overflow: "scroll",
          margin: "0px",
        }}
        wrapLongLines={codeWrap}
      >
        {code}
      </SyntaxHighlighter>
    </Box>
  );
}
