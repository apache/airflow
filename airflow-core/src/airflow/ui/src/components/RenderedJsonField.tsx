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
import Editor, { type OnMount } from "@monaco-editor/react";
import { useCallback, useState } from "react";

import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";

const MAX_HEIGHT = 300;
const MIN_HEIGHT = 40;

type Props = {
  readonly collapsed?: boolean;
  readonly content: object;
  readonly enableClipboard?: boolean;
} & FlexProps;

const RenderedJsonField = ({ collapsed = false, content, enableClipboard = true, ...rest }: Props) => {
  const contentFormatted = JSON.stringify(content, undefined, 2);
  const { colorMode } = useColorMode();
  const lineCount = contentFormatted.split("\n").length;
  const expandedHeight = Math.min(Math.max(lineCount * 19 + 10, MIN_HEIGHT), MAX_HEIGHT);
  const [editorHeight, setEditorHeight] = useState(collapsed ? MIN_HEIGHT : expandedHeight);
  const [isReady, setIsReady] = useState(!collapsed);
  const theme = colorMode === "dark" ? "vs-dark" : "vs-light";

  const handleMount: OnMount = useCallback(
    (editorInstance) => {
      editorInstance.onDidContentSizeChange(() => {
        const contentHeight = editorInstance.getContentHeight();

        setEditorHeight(Math.min(Math.max(contentHeight, MIN_HEIGHT), MAX_HEIGHT));
      });

      if (collapsed) {
        const action = editorInstance.getAction("editor.foldAll");

        if (action) {
          void action.run().then(() => {
            setIsReady(true);
          });
        } else {
          setIsReady(true);
        }
      }
    },
    [collapsed],
  );

  return (
    <Flex
      flex={1}
      gap={2}
      minW={200}
      // Hide the editor until it's ready to prevent a flickering effect when collapsing.
      // The editor will be hidden until the fold action is completed (if collapsed) or immediately if not collapsed.
      style={isReady ? undefined : { height: "0px", overflow: "hidden" }}
      {...rest}
    >
      <Editor
        height={`${editorHeight}px`}
        language="json"
        onMount={handleMount}
        options={{
          automaticLayout: true,
          contextmenu: false,
          folding: true,
          fontSize: 13,
          glyphMargin: false,
          lineDecorationsWidth: 0,
          lineNumbers: "off",
          minimap: { enabled: false },
          overviewRulerLanes: 0,
          readOnly: true,
          renderLineHighlight: "none",
          scrollbar: { vertical: "hidden", verticalScrollbarSize: 0 },
          scrollBeyondLastLine: false,
          wordWrap: "on",
        }}
        theme={theme}
        value={contentFormatted}
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
