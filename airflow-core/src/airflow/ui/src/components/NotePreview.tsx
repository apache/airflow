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
import { Box, Button, HStack } from "@chakra-ui/react";
import { type MouseEvent, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiEdit } from "react-icons/fi";

import MarkdownModal from "src/components/MarkdownModal";
import NoteIcon from "src/components/NoteIcon";
import ReactMarkdown from "src/components/ReactMarkdown";
import { IconButton } from "src/components/ui";

type Props = {
  readonly header: string;
  readonly isPending: boolean;
  readonly note: string | null;
  readonly onOpen: () => void;
  readonly onSave: () => void;
  readonly setNote: (value: string) => void;
};

export const NotePreview = ({ header, isPending, note, onOpen, onSave, setNote }: Props) => {
  const { t: translate } = useTranslation();
  const [isOpen, setIsOpen] = useState(false);

  const hasNote = note !== null && note.trim().length > 0;
  // Only the first non-empty line drives the single-line preview; append an
  // ellipsis when there's more content below it that the preview hides.
  const meaningfulLines = hasNote ? note.split("\n").filter((line) => line.trim().length > 0) : [];
  const firstLine = meaningfulLines[0] ?? "";
  const previewContent = meaningfulLines.length > 1 ? `${firstLine} …` : firstLine;

  const handleOpen = () => {
    onOpen();
    setIsOpen(true);
  };

  // The whole row opens the editor, but the preview renders real markdown links. Without this a
  // click on one would follow the link and open the editor over the departing page.
  const handleRowClick = (event: MouseEvent<HTMLDivElement>) => {
    if (!(event.target instanceof Element) || event.target.closest("a") === null) {
      handleOpen();
    }
  };

  return (
    <>
      <HStack
        alignItems="center"
        bg={hasNote ? "bg.info" : undefined}
        borderRadius="md"
        cursor={hasNote ? "pointer" : undefined}
        gap={2}
        onClick={hasNote ? handleRowClick : undefined}
        p={hasNote ? 3 : undefined}
      >
        {hasNote ? (
          <Box color="brand.solid" flexShrink={0} fontSize="md">
            <NoteIcon hasNote />
          </Box>
        ) : null}

        {/* Read-only single-line preview — links stay clickable, edits go through the modal */}
        <Box
          flex="1"
          fontSize="sm"
          overflow="hidden"
          style={{
            display: "-webkit-box",
            WebkitBoxOrient: "vertical",
            WebkitLineClamp: 1,
            whiteSpace: "normal",
          }}
        >
          {hasNote ? (
            <ReactMarkdown>{previewContent}</ReactMarkdown>
          ) : (
            <Button onClick={handleOpen} size="xs" variant="ghost">
              <FiEdit />
              {translate("note.placeholder")}
            </Button>
          )}
        </Box>

        {hasNote ? (
          <IconButton
            bg="bg"
            flexShrink={0}
            label={translate("note.edit")}
            onClick={(evt) => {
              evt.stopPropagation();
              handleOpen();
            }}
            size="xs"
            variant="outline"
          >
            <FiEdit />
          </IconButton>
        ) : null}
      </HStack>
      {/* Outside the row on purpose: React bubbles events out of a portal along the component tree,
          so a click inside the modal would also fire the row's onClick — resetting the draft note
          and reopening the modal on top of the save. */}
      <MarkdownModal
        header={header}
        isOpen={isOpen}
        isPending={isPending}
        mdContent={note}
        onClose={() => setIsOpen(false)}
        onConfirm={onSave}
        placeholder={translate("note.placeholder")}
        setMdContent={setNote}
      />
    </>
  );
};
