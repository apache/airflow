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
import { Badge, Button, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Tooltip } from "src/system-components";

import type { JsonPreviewEntry } from "src/utils/jsonPreview";

const MAX_VALUE_CHARS = 40;
// Below this a shortened value says nothing, so a narrow cap spends its width on fewer badges
// rather than on unreadable ones.
const MIN_VALUE_CHARS = 12;
// Widest advance of one glyph in the 12px badge font, measured across real payloads. Estimating
// high costs a little column width; estimating low wraps the preview onto a second line.
const CHAR_WIDTH = 7.3;
// A badge's own padding and inner gap, plus the gap that follows it.
const BADGE_CHROME = 20;
const MORE_BUTTON_WIDTH = 64;
// The preview asks for the width its badges need so a table column widens rather than stacking
// them, but never so much that it starves the columns beside it.
const MAX_WIDTH = 380;

type Props = {
  readonly entries: Array<JsonPreviewEntry>;
  /** Lower cap for a table that carries more than one of these side by side. */
  readonly maxWidth?: number;
  readonly onExpand: () => void;
};

export const JsonPreviewBadges = ({ entries, maxWidth = MAX_WIDTH, onExpand }: Props) => {
  const { t: translate } = useTranslation(["components", "common"]);

  const previews = entries.map((entry) => {
    const full =
      entry.itemCount === undefined
        ? entry.value.replaceAll(/\s+/gu, " ")
        : translate("jsonPreview.items", { count: entry.itemCount });
    // A badge sets its value on one line, so one long entry — a dotted class path, a URL — would
    // carry the whole preview past the cap on its own, whatever the loop below decides to show.
    const labelChars = entry.label === undefined ? 0 : entry.label.length + 1;
    const roomChars = Math.floor((maxWidth - BADGE_CHROME) / CHAR_WIDTH) - labelChars;
    const valueChars = Math.max(MIN_VALUE_CHARS, Math.min(MAX_VALUE_CHARS, roomChars));
    const text = full.length > valueChars ? `${full.slice(0, valueChars)}…` : full;

    // A summarised or shortened value only hints at what it stands for, so the editor still has
    // something to add even when every entry is on screen.
    return { ...entry, full, isElided: entry.isComplex || text !== full, text };
  });

  // Badges fill a single line; the first one that would not fit, and everything after it, is left
  // to the expand button — which has to fit on that same line too.
  const visibleEntries: Array<(typeof previews)[number]> = [];
  let usedWidth = 0;

  for (const [index, preview] of previews.entries()) {
    const labelChars = preview.label === undefined ? 0 : preview.label.length + 1;
    const badgeWidth = (preview.text.length + labelChars) * CHAR_WIDTH + BADGE_CHROME;
    const needsButton =
      index < previews.length - 1 || preview.isElided || visibleEntries.some((shown) => shown.isElided);

    if (
      visibleEntries.length > 0 &&
      usedWidth + badgeWidth + (needsButton ? MORE_BUTTON_WIDTH : 0) > maxWidth
    ) {
      break;
    }

    visibleEntries.push(preview);
    usedWidth += badgeWidth;
  }

  const hiddenCount = previews.length - visibleEntries.length;
  // Nothing left to reveal means nothing to expand.
  const canExpand = hiddenCount > 0 || visibleEntries.some((preview) => preview.isElided);
  const previewWidth = Math.min(usedWidth + (canExpand ? MORE_BUTTON_WIDTH : 0), maxWidth);
  // The expand button shares the line, so a badge that filled the cap on its own would still carry
  // the preview — and the column around it — past that cap.
  const badgeMaxWidth = maxWidth - (canExpand ? MORE_BUTTON_WIDTH : 0);

  return (
    <HStack data-testid="json-preview-badges" gap={1} minW={`${previewWidth}px`}>
      {visibleEntries.map(({ full, id, isComplex, label, text }) => (
        <Tooltip content={full} disabled={text === full} key={id} portalled>
          {/* The character budget above only estimates how wide a badge will be, and a font that
              runs wider than the estimate would carry the badge — and the column holding it — past
              the cap. maxW is what actually holds the line. */}
          <Badge
            colorPalette="gray"
            gap={1}
            maxW={`${badgeMaxWidth}px`}
            overflow="hidden"
            size="sm"
            variant="surface"
            whiteSpace="nowrap"
          >
            {label === undefined ? undefined : (
              <Text as="span" color="fg.muted">
                {label}:
              </Text>
            )}
            <Text
              as="span"
              color={isComplex ? "fg.muted" : "fg"}
              fontFamily="mono"
              overflow="hidden"
              textOverflow="ellipsis"
            >
              {text}
            </Text>
          </Badge>
        </Tooltip>
      ))}
      {canExpand ? (
        <Button
          // A filled chip so the control reads as a control on any row, header, or stripe behind
          // it, and badge-height so it sits inline with them rather than growing the row.
          colorPalette="gray"
          data-testid="json-preview-more"
          fontSize="xs"
          h={5}
          onClick={onExpand}
          px={1.5}
          size="xs"
          variant="subtle"
        >
          {hiddenCount > 0
            ? translate("limitedList", { count: hiddenCount })
            : translate("common:expand.expand")}
        </Button>
      ) : undefined}
    </HStack>
  );
};
