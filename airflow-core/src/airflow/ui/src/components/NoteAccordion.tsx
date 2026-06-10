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
import { Box, Heading, HStack } from "@chakra-ui/react";
import type { PropsWithChildren } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { MdExpandMore } from "react-icons/md";
import { PiNoteBold, PiNoteBlankBold } from "react-icons/pi";

import ReactMarkdown from "src/components/ReactMarkdown";
import { IconButton } from "src/components/ui";

import EditableMarkdownArea from "./EditableMarkdownArea";

type Props = {
  readonly note: string | null;
  readonly onSave: () => void;
  readonly setNote: (value: string) => void;
};

// Compact heading overrides — same visual weight as body text, just bold
const compactHeadingComponents = {
  h1: ({ children }: PropsWithChildren) => (
    <Heading as="h1" fontSize="1em" fontWeight="bold" my={1}>
      {children}
    </Heading>
  ),
  h2: ({ children }: PropsWithChildren) => (
    <Heading as="h2" fontSize="0.9em" fontWeight="bold" my={1}>
      {children}
    </Heading>
  ),
  h3: ({ children }: PropsWithChildren) => (
    <Heading as="h3" fontSize="0.85em" fontWeight="bold" my={1}>
      {children}
    </Heading>
  ),
};

const NoteAccordion = ({ note, onSave, setNote }: Props) => {
  const { t: translate } = useTranslation();
  const [isExpanded, setIsExpanded] = useState(false);
  const [isEditing, setIsEditing] = useState(false);

  const hasNote = note !== null && note.trim().length > 0;
  const firstLine = hasNote ? (note.split("\n")[0] ?? "") : "";

  // Content can be expanded if it has more than one non-empty line, or the first line is long
  const meaningfulLines = hasNote
    ? note
        .trim()
        .split("\n")
        .filter((line) => line.trim().length > 0)
    : [];
  const canExpand = hasNote && (meaningfulLines.length > 1 || firstLine.length > 80);

  // While editing: always show full content, suppress expand/collapse chrome
  // After blur: canExpand and isExpanded drive visibility normally
  const showContent = isEditing || !hasNote || !canExpand || isExpanded;
  const showCollapsedPreview = !isEditing && hasNote && canExpand && !isExpanded;
  const showChevron = !isEditing && canExpand;

  return (
    <HStack alignItems="flex-start" gap={2} px={3} py={2}>
      {/* Icon pinned to top-left */}
      <Box color={hasNote ? undefined : "fg.subtle"} flexShrink={0} fontSize="md" pt="2px">
        {hasNote ? <PiNoteBold /> : <PiNoteBlankBold />}
      </Box>

      <Box flex="1" fontSize="sm">
        {/* Collapsed preview — only for multiline notes that haven't been expanded */}
        {showCollapsedPreview ? (
          <Box
            cursor="pointer"
            onClick={() => setIsExpanded(true)}
            overflow="hidden"
            style={{
              display: "-webkit-box",
              overflow: "hidden",
              WebkitBoxOrient: "vertical",
              WebkitLineClamp: 1,
              whiteSpace: "normal",
            }}
          >
            <ReactMarkdown components={compactHeadingComponents}>{firstLine}</ReactMarkdown>
          </Box>
        ) : undefined}

        {/*
         * EditableMarkdownArea stays mounted at the same tree position always.
         * Visibility is driven by the grid trick so React never unmounts it —
         * this preserves focus when hasNote flips from false→true on first keystroke.
         * Single-line notes always have showContent=true so they're directly clickable.
         */}
        <Box
          style={{
            display: "grid",
            gridTemplateRows: showContent ? "1fr" : "0fr",
            transition: canExpand ? "grid-template-rows 0.2s ease" : undefined,
          }}
        >
          <Box maxH="240px" overflow="auto">
            <EditableMarkdownArea
              autoSize
              components={compactHeadingComponents}
              mdContent={note}
              onBlur={() => {
                setIsEditing(false);
                onSave();
              }}
              onFocus={() => {
                setIsEditing(true);
                setIsExpanded(true);
              }}
              padding={0}
              placeholder={translate("note.placeholder")}
              setMdContent={setNote}
            />
          </Box>
        </Box>
      </Box>

      {/* Chevron only after save, when content spans more than one line */}
      {showChevron ? (
        <IconButton
          flexShrink={0}
          label={isExpanded ? translate("expand.collapse") : translate("expand.expand")}
          onClick={() => setIsExpanded((prev) => !prev)}
          size="xs"
          variant="ghost"
        >
          <Box
            style={{
              display: "flex",
              transform: isExpanded ? "rotate(180deg)" : "rotate(0deg)",
              transition: "transform 0.2s ease",
            }}
          >
            <MdExpandMore />
          </Box>
        </IconButton>
      ) : undefined}
    </HStack>
  );
};

export default NoteAccordion;
