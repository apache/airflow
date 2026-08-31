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
import { Box, Flex, type FlexProps, VStack } from "@chakra-ui/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiChevronUp } from "react-icons/fi";

import { ClipboardRoot, ClipboardIconButton, IconButton } from "src/system-components";

import { JsonPreviewBadges } from "src/components/JsonPreviewBadges";
import Editor, { type OnMount } from "src/components/MonacoEditor";
import { useMonacoTheme } from "src/context/colorMode";
import { useContainerWidth } from "src/utils";
import { getJsonPreviewEntries } from "src/utils/jsonPreview";

const MAX_HEIGHT = 300;
const MIN_HEIGHT = 40;
const MIN_WIDTH = 200;
// Wide enough for most payloads to stop wrapping, narrow enough to leave a table's other columns room.
const MAX_WIDTH = 700;
// Approximate advance of one glyph in the editor's 13px monospace font.
const CHAR_WIDTH = 7.8;
// The editor's own folding gutter, which sits left of the first character.
const EDITOR_GUTTER = 30;

// `content` is the JSON payload here, so Flex's CSS `content` prop is dropped from the props.
type Props = {
  readonly collapsed?: boolean;
  readonly content: object;
  readonly enableClipboard?: boolean;
} & Omit<FlexProps, "content">;

type EditorInstance = Parameters<OnMount>[0];

const RenderedJsonField = ({ collapsed, content, enableClipboard = true, ...rest }: Props) => {
  const contentFormatted = JSON.stringify(content, undefined, 2);
  const { t: translate } = useTranslation("common");
  const { beforeMount, theme } = useMonacoTheme();
  const lines = contentFormatted.split("\n");
  const expandedHeight = Math.min(Math.max(lines.length * 19 + 10, MIN_HEIGHT), MAX_HEIGHT);
  // An expanded editor asks for the width its longest line needs, so a table column widens to fit
  // instead of wrapping the JSON. Anything past MAX_WIDTH still wraps.
  const longestLine = lines.reduce((longest, line) => Math.max(longest, line.length), 0);
  const editorWidth = Math.min(Math.max(longestLine * CHAR_WIDTH + EDITOR_GUTTER, MIN_WIDTH), MAX_WIDTH);

  const previewEntries = getJsonPreviewEntries(content);
  const [isExpanded, setIsExpanded] = useState(collapsed !== true);
  const [lastCollapsed, setLastCollapsed] = useState(collapsed);

  if (collapsed !== lastCollapsed) {
    setLastCollapsed(collapsed);
    setIsExpanded(collapsed !== true);
  }

  // Only a field the caller can collapse gets a preview; the always-expanded ones stay plain editors.
  const showBadges = collapsed !== undefined && !isExpanded;

  const [editorHeight, setEditorHeight] = useState(expandedHeight);
  const [editor, setEditor] = useState<EditorInstance | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const containerWidth = useContainerWidth(containerRef);

  const handleMount: OnMount = useCallback((editorInstance) => {
    setEditor(editorInstance);

    editorInstance.onDidContentSizeChange(() => {
      const contentHeight = editorInstance.getContentHeight();

      setEditorHeight(Math.min(Math.max(contentHeight, MIN_HEIGHT), MAX_HEIGHT));
    });

    editorInstance.onDidDispose(() => {
      setEditor(null);
    });
  }, []);

  // A collapsed field renders no editor at all, so expanding one mounts Monaco into a container it
  // has never measured. If that measurement lands before the row is laid out, the editor settles at
  // a few pixels and stays there: `automaticLayout` only reacts to resizes that come after. Handing
  // it the container's real size once both the editor and that size are known fixes the first
  // layout, whichever of the two arrives last.
  useEffect(() => {
    const container = containerRef.current;

    if (editor === null || container === null) {
      return;
    }
    editor.layout({ height: container.clientHeight, width: container.clientWidth });
  }, [containerWidth, editor, editorHeight]);

  // An empty payload has nothing to say in a badge and nothing to show in an editor.
  if (previewEntries === undefined) {
    return undefined;
  }

  const clipboardButton = enableClipboard ? (
    <ClipboardRoot value={contentFormatted}>
      <ClipboardIconButton h={7} minW={7} />
    </ClipboardRoot>
  ) : undefined;

  if (showBadges) {
    return (
      <Flex alignItems="center" flex={1} gap={1} minW={`${MIN_WIDTH}px`} {...rest}>
        <JsonPreviewBadges entries={previewEntries} onExpand={() => setIsExpanded(true)} />
        {clipboardButton}
      </Flex>
    );
  }

  return (
    <Flex flex={1} gap={2} minW={`${MIN_WIDTH}px`} {...rest}>
      <Box flex={1} minW={`${editorWidth}px`} ref={containerRef}>
        <Editor
          beforeMount={beforeMount}
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
      </Box>
      <VStack gap={1}>
        {collapsed === undefined ? undefined : (
          <IconButton
            data-testid="json-preview-collapse"
            h={7}
            label={translate("expand.collapse")}
            minW={7}
            onClick={() => setIsExpanded(false)}
            size="xs"
          >
            <FiChevronUp />
          </IconButton>
        )}
        {clipboardButton}
      </VStack>
    </Flex>
  );
};

export default RenderedJsonField;
