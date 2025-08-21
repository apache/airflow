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
import { Flex, type FlexProps } from "@chakra-ui/react";
import ReactJson, { type ReactJsonViewProps } from "react-json-view";

import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";

type Props = {
  readonly content: object;
  readonly enableClipboard?: boolean;
  readonly jsonProps?: Omit<ReactJsonViewProps, "src">;
} & FlexProps;

const RenderedJsonField = ({ content, enableClipboard = true, jsonProps, ...rest }: Props) => {
  const contentFormatted = JSON.stringify(content, undefined, 4);
  const { colorMode } = useColorMode();

  return (
    <Flex {...rest}>
      <ReactJson
        displayDataTypes={false}
        enableClipboard={false}
        iconStyle="triangle"
        indentWidth={2}
        name={false}
        src={content}
        style={{
          backgroundColor: "inherit",
        }}
        theme={colorMode === "dark" ? "monokai" : "rjv-default"}
        {...jsonProps}
      />
      {enableClipboard ? (
        <ClipboardRoot value={contentFormatted}>
          <ClipboardIconButton h={7} minW={7} />
        </ClipboardRoot>
      ) : undefined}
    </Flex>
  );
};

export default RenderedJsonField;
