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
import { Box, Heading, VStack, Editable, Text, Flex } from "@chakra-ui/react";
import { type ChangeEvent, type ReactElement, useState } from "react";

import { Button, Dialog } from "src/components/ui";

import ReactMarkdown from "./ReactMarkdown";
import ActionButton from "./ui/ActionButton";

const EditableMarkdownButton = ({
  header,
  icon,
  isPending,
  mdContent,
  onConfirm,
  placeholder,
  setMdContent,
  text,
  withText = true,
}: {
  readonly header: string;
  readonly icon: ReactElement;
  readonly isPending: boolean;
  readonly mdContent?: string | null;
  readonly onConfirm: () => void;
  readonly placeholder: string;
  readonly setMdContent: (value: string) => void;
  readonly text: string;
  readonly withText?: boolean;
}) => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <Box>
      <ActionButton
        actionName={placeholder}
        icon={icon}
        onClick={() => setIsOpen(true)}
        text={text}
        withText={withText}
      />
      <Dialog.Root
        data-testid="markdown-modal"
        lazyMount
        onOpenChange={() => setIsOpen(false)}
        open={isOpen}
        size="md"
      >
        <Dialog.Content backdrop>
          <Dialog.Header bg="blue.muted">
            <Heading size="xl">{header}</Heading>
            <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
          </Dialog.Header>
          <Dialog.Body alignItems="flex-start" as={VStack} gap="0">
            <Editable.Root
              onChange={(event: ChangeEvent<HTMLInputElement>) => setMdContent(event.target.value)}
              value={mdContent ?? ""}
            >
              <Editable.Preview
                _hover={{ backgroundColor: "transparent" }}
                alignItems="flex-start"
                as={VStack}
                gap="0"
                height="200px"
                overflowY="auto"
                width="100%"
              >
                {Boolean(mdContent) ? (
                  <ReactMarkdown>{mdContent}</ReactMarkdown>
                ) : (
                  <Text color="fg.subtle">{placeholder}</Text>
                )}
              </Editable.Preview>
              <Editable.Textarea
                data-testid="markdown-input"
                height="200px"
                overflowY="auto"
                placeholder={placeholder}
                resize="none"
              />
            </Editable.Root>

            <Flex justifyContent="end" mt={3} width="100%">
              <Button
                colorPalette="blue"
                loading={isPending}
                onClick={() => {
                  onConfirm();
                  setIsOpen(false);
                }}
              >
                {icon} Confirm
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};

export default EditableMarkdownButton;
