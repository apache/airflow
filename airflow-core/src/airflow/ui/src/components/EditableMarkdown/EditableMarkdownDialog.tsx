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
import { Box, CloseButton, Dialog, Flex, Heading, VStack } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { Button } from "src/components/ui";
import { MARKDOWN_DIALOG_STORAGE_KEY, ResizableWrapper } from "src/components/ui/ResizableWrapper";

import { EditableMarkdownArea } from "./EditableMarkdownArea";

type EditableMarkdownDialogProps = {
  readonly header: string;
  readonly icon?: ReactNode;
  readonly isPending: boolean;
  readonly mdContent?: string | null;
  readonly onClose: () => void;
  readonly onConfirm: () => void;
  readonly open: boolean;
  readonly placeholder: string;
  readonly setMdContent: (value: string) => void;
};

export const EditableMarkdownDialog = ({
  header,
  icon,
  isPending,
  mdContent,
  onClose,
  onConfirm,
  open,
  placeholder,
  setMdContent,
}: EditableMarkdownDialogProps) => {
  const { t: translate } = useTranslation("common");

  return (
    <Dialog.Root
      data-testid="markdown-modal"
      lazyMount
      onOpenChange={() => onClose()}
      open={open}
      size="md"
      unmountOnExit={true}
    >
      <Dialog.Backdrop />
      <Dialog.Positioner>
        <Dialog.Content maxHeight="90vh" maxWidth="90vw" padding={0} width="auto">
          <ResizableWrapper storageKey={MARKDOWN_DIALOG_STORAGE_KEY}>
            <Dialog.Header bg="brand.muted" flexShrink={0}>
              <Heading size="xl">{header}</Heading>
              <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
                <CloseButton size="xl" />
              </Dialog.CloseTrigger>
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
                      onClose();
                    }}
                  >
                    {icon} {translate("modal.confirm")}
                  </Button>
                </Flex>
              </Box>
            </Dialog.Body>
          </ResizableWrapper>
        </Dialog.Content>
      </Dialog.Positioner>
    </Dialog.Root>
  );
};
