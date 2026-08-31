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
  readonly onExpand: () => void;
};

export const JsonPreviewBadges = ({ entries, onExpand }: Props) => {
  const { t: translate } = useTranslation(["components", "common"]);

  const previews = entries.map((entry) => {
    const full =
      entry.itemCount === undefined
        ? entry.value.replaceAll(/\s+/gu, " ")
        : translate("jsonPreview.items", { count: entry.itemCount });
    const text = full.length > MAX_VALUE_CHARS ? `${full.slice(0, MAX_VALUE_CHARS)}…` : full;

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
      usedWidth + badgeWidth + (needsButton ? MORE_BUTTON_WIDTH : 0) > MAX_WIDTH
    ) {
      break;
    }

    visibleEntries.push(preview);
    usedWidth += badgeWidth;
  }

  const hiddenCount = previews.length - visibleEntries.length;
  // Nothing left to reveal means nothing to expand.
  const canExpand = hiddenCount > 0 || visibleEntries.some((preview) => preview.isElided);
  const previewWidth = Math.min(usedWidth + (canExpand ? MORE_BUTTON_WIDTH : 0), MAX_WIDTH);

  return (
    <HStack data-testid="json-preview-badges" gap={1} minW={`${previewWidth}px`}>
      {visibleEntries.map(({ full, id, isComplex, label, text }) => (
        <Tooltip content={full} disabled={text === full} key={id} portalled>
          <Badge colorPalette="gray" gap={1} size="sm" variant="surface" whiteSpace="nowrap">
            {label === undefined ? undefined : (
              <Text as="span" color="fg.muted">
                {label}:
              </Text>
            )}
            <Text as="span" color={isComplex ? "fg.muted" : "fg"} fontFamily="mono">
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
