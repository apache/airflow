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
import { Box, Input, Button, Dialog, CloseButton } from "@chakra-ui/react";
import { Select as ReactSelect, type SingleValue } from "chakra-react-select";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { useDagServiceGetDagTags } from "openapi/queries";

type AdvancedSearchProps = {
  readonly initialValue: string;
  readonly isOpen: boolean;
  readonly onClose: () => void;
  readonly onSubmit: (query: string) => void;
};

export const AdvancedSearch = ({ initialValue, isOpen, onClose, onSubmit }: AdvancedSearchProps) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const inputRef = useRef<HTMLInputElement>(null);
  const [inputValue, setInputValue] = useState(initialValue);
  const { data: tagsData, isLoading } = useDagServiceGetDagTags({ orderBy: "name" });
  const availableTags = tagsData?.tags ?? [];

  useEffect(() => {
    setInputValue(initialValue);
  }, [initialValue, isOpen]);

  const handleTagClick = (tag: string) => {
    if (!inputRef.current) {
      return;
    }
    const input = inputRef.current;
    const start = input.selectionStart ?? 0;
    const end = input.selectionEnd ?? 0;
    const newValue = inputValue.slice(0, start) + tag + inputValue.slice(end);

    setInputValue(newValue);
    setTimeout(() => {
      input.focus();
      input.setSelectionRange(start + tag.length, start + tag.length);
    }, 0);
  };

  const handleSubmit = () => onSubmit(inputValue);
  const handleClear = () => {
    setInputValue("");
    onSubmit("");
  };

  return (
    <Dialog.Root open={isOpen}>
      <Dialog.Backdrop />
      <Dialog.Positioner>
        <Dialog.Content>
          <Dialog.Header>
            <Dialog.Title>{translate("list.advancedSearch")}</Dialog.Title>
          </Dialog.Header>
          <Dialog.Body>
            <Box mb={4}>
              <Input
                onChange={(event) => setInputValue(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    handleSubmit();
                  }
                }}
                placeholder={translate("Advanced tag query, e.g. (tag1 OR tag2) AND tag3")}
                ref={inputRef}
                value={inputValue}
              />
            </Box>
            <Box display="flex" flexDirection="row" gap={3}>
              <Box minW="160px">
                <ReactSelect
                  aria-label={translate("list.selectTag", { defaultValue: "Add Tag" })}
                  isClearable={false}
                  isDisabled={isLoading}
                  isMulti={false}
                  onChange={(option: SingleValue<{ label: string; value: string }>) => {
                    if (typeof option?.value === "string") {
                      handleTagClick(option.value);
                    }
                  }}
                  options={availableTags.map((tag) => ({ label: tag, value: tag }))}
                  placeholder={translate("list.selectTag", { defaultValue: "Add Tag" })}
                  value={null}
                />
              </Box>
            </Box>
            <Box display="flex" gap={4} justifyContent="flex-end">
              <Button colorScheme="gray" onClick={handleClear} variant="outline">
                {translate("common:clear")}
              </Button>
              <Button colorScheme="gray" onClick={handleSubmit} variant="outline">
                {translate("common:enter")}
              </Button>
            </Box>
          </Dialog.Body>
          <Dialog.CloseTrigger>
            <CloseButton onClick={onClose} size="sm" />
          </Dialog.CloseTrigger>
        </Dialog.Content>
      </Dialog.Positioner>
    </Dialog.Root>
  );
};
