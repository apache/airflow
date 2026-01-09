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
import { Field, HStack, Text } from "@chakra-ui/react";
import { Select as ReactSelect, type MultiValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";

import { Switch } from "src/components/ui";

type Props = {
  readonly onMenuScrollToBottom: () => void;
  readonly onMenuScrollToTop: () => void;
  readonly onSelectTagsChange: (tags: MultiValue<{ label: string; value: string }>) => void;
  readonly onTagModeChange: ({ checked }: { checked: boolean }) => void;
  readonly onUpdate: (newValue: string) => void;
  readonly selectedTags: Array<string>;
  readonly tagFilterMode: string;
  readonly tags: Array<string>;
};

export const TagFilter = ({
  onMenuScrollToBottom,
  onMenuScrollToTop,
  onSelectTagsChange,
  onTagModeChange,
  onUpdate,
  selectedTags,
  tagFilterMode,
  tags,
}: Props) => {
  const { t: translate } = useTranslation("common");

  return (
    <>
      <Field.Root>
        <ReactSelect
          aria-label={translate("table.filterByTag")}
          chakraStyles={{
            clearIndicator: (provided) => ({
              ...provided,
              color: "gray.fg",
            }),
            container: (provided) => ({
              ...provided,
              maxWidth: 300,
              minWidth: 64,
            }),
            control: (provided) => ({
              ...provided,
              colorPalette: "brand",
            }),
            menu: (provided) => ({
              ...provided,
              zIndex: 2,
            }),
          }}
          isClearable
          isMulti
          noOptionsMessage={() => translate("table.noTagsFound")}
          onChange={onSelectTagsChange}
          onInputChange={(newValue) => onUpdate(newValue)}
          onMenuScrollToBottom={onMenuScrollToBottom}
          onMenuScrollToTop={onMenuScrollToTop}
          options={tags.map((tag) => ({
            label: tag,
            value: tag,
          }))}
          placeholder={translate("table.tagPlaceholder")}
          value={selectedTags.map((tag) => ({
            label: tag,
            value: tag,
          }))}
        />
      </Field.Root>
      {selectedTags.length >= 2 && (
        <HStack align="center" gap={1}>
          <Text
            color={tagFilterMode === "any" ? "fg.info" : "fg.muted"}
            fontSize="sm"
            fontWeight={tagFilterMode === "any" ? "bold" : "normal"}
          >
            {translate("table.tagMode.any")}
          </Text>
          <Switch checked={tagFilterMode === "all"} onCheckedChange={onTagModeChange} variant="raised" />
          <Text
            color={tagFilterMode === "all" ? "fg.info" : "fg.muted"}
            fontSize="sm"
            fontWeight={tagFilterMode === "all" ? "bold" : "normal"}
          >
            {translate("table.tagMode.all")}
          </Text>
        </HStack>
      )}
    </>
  );
};
