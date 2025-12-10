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
import { Box, CloseButton, Dialog, Heading, VStack } from "@chakra-ui/react";
import { type ReactElement, useState } from "react";

import { Button } from "src/components/ui";
import { MARKDOWN_DIALOG_STORAGE_KEY, ResizableWrapper } from "src/components/ui/ResizableWrapper";

import ReactMarkdown from "./ReactMarkdown";

const DisplayMarkdownButton = ({
  header,
  icon,
  mdContent,
  text,
}: {
  readonly header: string;
  readonly icon?: ReactElement;
  readonly mdContent: string;
  readonly text: string;
}) => {
  const [isDocsOpen, setIsDocsOpen] = useState(false);

  return (
    <Box>
      <Button data-testid="markdown-button" onClick={() => setIsDocsOpen(true)} variant="outline">
        {icon}
        {text}
      </Button>
      <Dialog.Root
        data-testid="markdown-modal"
        onOpenChange={() => setIsDocsOpen(false)}
        open={isDocsOpen}
        size="md"
      >
        <Dialog.Backdrop />
        <Dialog.Positioner>
          <Dialog.Content maxHeight="none" maxWidth="none" padding={0} width="auto">
            <ResizableWrapper storageKey={MARKDOWN_DIALOG_STORAGE_KEY}>
              <Dialog.Header bg="brand.muted" flexShrink={0}>
                <Heading size="xl">{header}</Heading>
                <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
                  <CloseButton size="xl" />
                </Dialog.CloseTrigger>
              </Dialog.Header>
              <Dialog.Body alignItems="flex-start" as={VStack} flex="1" gap="0" overflow="auto">
                <ReactMarkdown>{mdContent}</ReactMarkdown>
              </Dialog.Body>
            </ResizableWrapper>
          </Dialog.Content>
        </Dialog.Positioner>
      </Dialog.Root>
    </Box>
  );
};

export default DisplayMarkdownButton;
