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
import { Box, Button, Flex, Heading, IconButton, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { PiNoteBold, PiNoteBlankBold } from "react-icons/pi";

import { Dialog, Tooltip } from "src/components/ui";
import { ResizableWrapper, MARKDOWN_DIALOG_STORAGE_KEY } from "src/components/ui/ResizableWrapper";

import EditableMarkdownArea from "./EditableMarkdownArea";

const EditableMarkdownButton = ({
  header,
  isPending,
  mdContent,
  onConfirm,
  onOpen,
  placeholder,
  setMdContent,
}: {
  readonly header: string;
  readonly isPending: boolean;
  readonly mdContent?: string | null;
  readonly onConfirm: () => void;
  readonly onOpen: () => void;
  readonly placeholder: string;
  readonly setMdContent: (value: string) => void;
}) => {
  const { t: translate } = useTranslation("common");
  const [isOpen, setIsOpen] = useState(false);

  const hasContent = Boolean(mdContent?.trim());
  const noteIcon = hasContent ? <PiNoteBold /> : <PiNoteBlankBold />;
  const label = hasContent ? translate("note.label") : translate("note.add");

  const handleOpen = () => {
    if (!isOpen) {
      onOpen();
    }
    setIsOpen(true);
  };

  return (
    <>
      <Tooltip content={label}>
        <IconButton aria-label={label} colorPalette="brand" onClick={handleOpen} size="md" variant="ghost">
          {noteIcon}
        </IconButton>
      </Tooltip>
      <Dialog.Root
        data-testid="markdown-modal"
        lazyMount
        onOpenChange={() => setIsOpen(false)}
        open={isOpen}
        size="md"
        unmountOnExit={true}
      >
        <Dialog.Content backdrop maxHeight="90vh" maxWidth="90vw" padding={0} width="auto">
          <ResizableWrapper storageKey={MARKDOWN_DIALOG_STORAGE_KEY}>
            <Dialog.Header bg="brand.muted" flexShrink={0}>
              <Heading size="xl">{header}</Heading>
              <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
            </Dialog.Header>
            <Dialog.Body alignItems="flex-start" as={VStack} flex="1" gap="0" overflow="hidden" p={0}>
              <Box flex="1" overflow="hidden" width="100%">
                <EditableMarkdownArea
                  mdContent={mdContent}
                  placeholder={placeholder}
                  setMdContent={setMdContent}
                />
              </Box>
              <Box bg="bg.panel" flexShrink={0} width="100%">
                <Flex justifyContent="end" p={4}>
                  <Button
                    colorPalette="brand"
                    loading={isPending}
                    onClick={() => {
                      onConfirm();
                      setIsOpen(false);
                    }}
                  >
                    {noteIcon} {translate("modal.confirm")}
                  </Button>
                </Flex>
              </Box>
            </Dialog.Body>
          </ResizableWrapper>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default EditableMarkdownButton;
