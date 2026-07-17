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
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { IconButton } from "src/components/ui";

import MarkdownModal from "./MarkdownModal";
import NoteIcon from "./NoteIcon";

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
  const label = hasContent ? translate("note.label") : translate("note.add");

  const handleOpen = () => {
    if (!isOpen) {
      onOpen();
    }
    setIsOpen(true);
  };

  return (
    <>
      <IconButton label={label} onClick={handleOpen}>
        <NoteIcon hasNote={hasContent} />
      </IconButton>
      <MarkdownModal
        header={header}
        isOpen={isOpen}
        isPending={isPending}
        mdContent={mdContent}
        onClose={() => setIsOpen(false)}
        onConfirm={onConfirm}
        placeholder={placeholder}
        setMdContent={setMdContent}
      />
    </>
  );
};

export default EditableMarkdownButton;
