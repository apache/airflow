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
import { Box, Button, Flex, Heading, HStack, Text, Textarea, VStack } from "@chakra-ui/react";
import type { ChangeEvent } from "react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiEdit, FiEye } from "react-icons/fi";

import NoteIcon from "src/components/NoteIcon";
import ReactMarkdown from "src/components/ReactMarkdown";
import { Dialog } from "src/components/ui";
import { ResizableWrapper, MARKDOWN_DIALOG_STORAGE_KEY } from "src/components/ui/ResizableWrapper";

export const MAX_NOTE_LENGTH = 1000;

const MarkdownModal = ({
  header,
  isOpen,
  isPending,
  mdContent,
  onClose,
  onConfirm,
  placeholder,
  setMdContent,
}: {
  readonly header: string;
  readonly isOpen: boolean;
  readonly isPending: boolean;
  readonly mdContent?: string | null;
  readonly onClose: () => void;
  readonly onConfirm: () => void;
  readonly placeholder: string;
  readonly setMdContent: (value: string) => void;
}) => {
  const { t: translate } = useTranslation("common");

  const hasContent = Boolean(mdContent?.trim());
  // Open straight into editing when there's nothing to read; otherwise show the
  // rendered note (links clickable) and let the edit button reveal the textarea.
  const [isEditing, setIsEditing] = useState(!hasContent);
  // While editing, toggle between the textarea and a rendered preview.
  const [showPreview, setShowPreview] = useState(false);

  // Track the saved content while closed; it freezes on open, so dismissing (X / Esc / backdrop)
  // restores it and discards the unsaved draft instead of leaking it into the shared note state.
  // Confirm closes through onClose directly and keeps the edited value.
  const openContentRef = useRef(mdContent ?? "");

  if (!isOpen) {
    openContentRef.current = mdContent ?? "";
  }

  const onDismiss = () => {
    setMdContent(openContentRef.current);
    onClose();
  };

  const length = mdContent?.length ?? 0;
  // Existing notes may already exceed the limit (created before validation or
  // via the API); block saving until trimmed rather than silently truncating.
  const isOverLimit = length > MAX_NOTE_LENGTH;

  // Textarea only while editing and not previewing; otherwise show the rendered markdown.
  const showEditor = isEditing && !showPreview;
  const renderedContent = hasContent ? (
    <ReactMarkdown>{mdContent ?? ""}</ReactMarkdown>
  ) : (
    <Text color="fg.subtle">{placeholder}</Text>
  );

  return (
    <Dialog.Root
      data-testid="markdown-modal"
      lazyMount
      onOpenChange={({ open: nextOpen }) => {
        if (!nextOpen) {
          onDismiss();
        }
      }}
      open={isOpen}
      size="xl"
      unmountOnExit={true}
    >
      <Dialog.Content backdrop maxHeight="90vh" maxWidth="90vw" padding={0} width="auto">
        <ResizableWrapper
          defaultSize={{ height: 700, width: 1000 }}
          maxConstraints={[1600, 1000]}
          minSize={{ height: 600, width: 800 }}
          storageKey={MARKDOWN_DIALOG_STORAGE_KEY}
        >
          <Dialog.Header bg="brand.muted" flexShrink={0}>
            <Heading size="xl">{header}</Heading>
            <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
          </Dialog.Header>
          <Dialog.Body alignItems="stretch" as={VStack} flex="1" gap="0" overflow="hidden" p={0}>
            <Box display="flex" flex="1" flexDirection="column" minH={0} overflow="hidden" p={4} width="100%">
              {showEditor ? (
                <Textarea
                  data-testid="markdown-input"
                  flex="1"
                  maxLength={MAX_NOTE_LENGTH}
                  minH={0}
                  onChange={(event: ChangeEvent<HTMLTextAreaElement>) => setMdContent(event.target.value)}
                  placeholder={placeholder}
                  resize="none"
                  value={mdContent ?? ""}
                />
              ) : (
                <Box flex="1" minH={0} overflow="auto">
                  {renderedContent}
                </Box>
              )}
            </Box>
            <Box bg="bg.panel" flexShrink={0} width="100%">
              <Flex alignItems="center" gap={4} justifyContent="space-between" p={4}>
                {isEditing ? (
                  <Text color={isOverLimit ? "fg.error" : "fg.muted"} fontSize="sm">
                    {length}/{MAX_NOTE_LENGTH}
                  </Text>
                ) : (
                  <Box />
                )}
                {isEditing ? (
                  <HStack gap={2}>
                    <Button
                      data-testid="preview-toggle"
                      onClick={() => setShowPreview((prev) => !prev)}
                      variant="outline"
                    >
                      {showPreview ? <FiEdit /> : <FiEye />}
                      {showPreview ? translate("note.write") : translate("note.preview")}
                    </Button>
                    <Button
                      disabled={isOverLimit}
                      loading={isPending}
                      onClick={() => {
                        onConfirm();
                        onClose();
                      }}
                    >
                      <NoteIcon hasNote={hasContent} /> {translate("modal.confirm")}
                    </Button>
                  </HStack>
                ) : (
                  <Button
                    data-testid="edit-markdown"
                    onClick={() => {
                      setShowPreview(false);
                      setIsEditing(true);
                    }}
                    variant="outline"
                  >
                    <FiEdit /> {translate("edit")}
                  </Button>
                )}
              </Flex>
            </Box>
          </Dialog.Body>
        </ResizableWrapper>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default MarkdownModal;
