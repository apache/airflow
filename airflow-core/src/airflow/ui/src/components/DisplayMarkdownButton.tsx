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
import { Box, Heading, VStack } from "@chakra-ui/react";
import { type ReactElement, useState } from "react";
import { ResizableBox } from "react-resizable";
import "react-resizable/css/styles.css";

import { Button, Dialog } from "src/components/ui";

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
        <Dialog.Content
          backdrop
          style={{
            maxHeight: "none",
            maxWidth: "none",
            padding: 0,
            width: "auto",
          }}
        >
          <ResizableBox
            height={600}
            maxConstraints={[1200, 800]}
            minConstraints={[512, 600]}
            resizeHandles={["se"]}
            style={{
              backgroundColor: "inherit",
              borderRadius: "inherit",
              overflow: "hidden",
              position: "relative",
            }}
            width={512}
          >
            <style>
              {`
                .react-resizable-handle-se {
                  width: 20px !important;
                  height: 20px !important;
                  bottom: 0 !important;
                  right: 0 !important;
                  background: linear-gradient(-45deg, transparent 6px, #ccc 6px, #ccc 8px, transparent 8px, transparent 12px, #ccc 12px, #ccc 14px, transparent 14px) !important;
                  cursor: se-resize !important;
                }
              `}
            </style>
            <Dialog.Header bg="brand.muted" flexShrink={0}>
              <Heading size="xl">{header}</Heading>
              <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
            </Dialog.Header>
            <Dialog.Body alignItems="flex-start" as={VStack} flex="1" gap="0" overflow="auto">
              <ReactMarkdown>{mdContent}</ReactMarkdown>
            </Dialog.Body>
          </ResizableBox>
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};

export default DisplayMarkdownButton;
