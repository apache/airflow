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
import { Box } from "@chakra-ui/react";
import { useState } from "react";
import { PiNoteBlankLight, PiNoteLight } from "react-icons/pi";

import ActionButton from "src/components/ui/ActionButton";

import { EditableMarkdownDialog } from "./EditableMarkdownDialog";

type EditableMarkdownButtonProps = {
  readonly header: string;
  readonly isPending: boolean;
  readonly mdContent?: string | null;
  readonly onConfirm: () => void;
  readonly onOpen: () => void;
  readonly placeholder: string;
  readonly setMdContent: (value: string) => void;
  readonly text: string;
  readonly withText?: boolean;
};

export const EditableMarkdownButton = ({
  header,
  isPending,
  mdContent,
  onConfirm,
  onOpen,
  placeholder,
  setMdContent,
  text,
  withText = true,
}: EditableMarkdownButtonProps) => {
  const [isOpen, setIsOpen] = useState(false);

  const noteIcon = Boolean(mdContent?.trim()) ? <PiNoteLight /> : <PiNoteBlankLight />;

  return (
    <>
      <Box display="inline-block" position="relative">
        <ActionButton
          actionName={placeholder}
          icon={noteIcon}
          onClick={() => {
            if (!isOpen) {
              onOpen();
            }
            setIsOpen(true);
          }}
          text={text}
          variant="outline"
          withText={withText}
        />
        {Boolean(mdContent?.trim()) && (
          <Box
            bg="brand.500"
            borderRadius="full"
            height={2.5}
            position="absolute"
            right={-0.5}
            top={-0.5}
            width={2.5}
          />
        )}
      </Box>
      <EditableMarkdownDialog
        header={header}
        icon={noteIcon}
        isPending={isPending}
        mdContent={mdContent}
        onClose={() => setIsOpen(false)}
        onConfirm={onConfirm}
        open={isOpen}
        placeholder={placeholder}
        setMdContent={setMdContent}
      />
    </>
  );
};
