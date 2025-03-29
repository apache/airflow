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
import { useTheme } from "next-themes";
import ReactJson, { type ReactJsonViewProps } from "react-json-view";

import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";

type Props = {
  readonly content: object;
  readonly jsonProps?: Omit<ReactJsonViewProps, "src">;
} & FlexProps;

const RenderedJsonField = ({ content, jsonProps, ...rest }: Props) => {
  const contentFormatted = JSON.stringify(content, undefined, 4);
  const { theme } = useTheme();

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
        theme={theme === "dark" ? "monokai" : "rjv-default"}
        {...jsonProps}
      />
      <ClipboardRoot value={contentFormatted}>
        <ClipboardIconButton />
      </ClipboardRoot>
    </Flex>
  );
};

export default RenderedJsonField;
