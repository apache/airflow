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

import React from "react";

import ReactJson from "react-json-view";

import { Flex, Button, Code, Spacer, useClipboard } from "@chakra-ui/react";

interface Props {
  content: string;
}

const RenderedJsonField = ({ content }: Props) => {
  let contentJson = null;
  let contentFormatted = "";
  let isJson = false;
  try {
    contentJson = JSON.parse(content);
    contentFormatted = JSON.stringify(contentJson, null, 4);
    isJson = true;
  } catch (e) {
    // skip
  }

  const { onCopy, hasCopied } = useClipboard(contentFormatted);

  let field = null;
  if (isJson) {
    field = (
      <Flex>
        <ReactJson
          src={contentJson}
          name={false}
          theme="rjv-default"
          iconStyle="triangle"
          indentWidth={2}
          displayDataTypes={false}
          enableClipboard={false}
          style={{ backgroundColor: "inherit" }}
        />
        <Spacer />
        <Button aria-label="Copy" onClick={onCopy}>
          {hasCopied ? "Copied!" : "Copy"}
        </Button>
      </Flex>
    );
  } else {
    field = <Code fontSize="md">{content}</Code>;
  }
  return field;
};

export default RenderedJsonField;
