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
import { CloseButton, HStack, IconButton, Input, InputGroup, Text } from "@chakra-ui/react";
import { useRef, type KeyboardEvent } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { FiChevronDown, FiChevronUp, FiSearch } from "react-icons/fi";

export type LogSearchInputProps = {
  readonly currentMatchIndex: number;
  readonly onSearchChange: (query: string) => void;
  readonly onSearchNext: () => void;
  readonly onSearchPrevious: () => void;
  readonly searchQuery: string;
  readonly totalMatches: number;
};

export const LogSearchInput = ({
  currentMatchIndex,
  onSearchChange,
  onSearchNext,
  onSearchPrevious,
  searchQuery,
  totalMatches,
}: LogSearchInputProps) => {
  const { t: translate } = useTranslation("dag");
  const searchInputRef = useRef<HTMLInputElement>(null);

  useHotkeys(
    "/",
    (event) => {
      event.preventDefault();
      searchInputRef.current?.focus();
    },
    { enableOnFormTags: false },
  );

  const handleSearchKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      if (event.shiftKey) {
        onSearchPrevious();
      } else {
        onSearchNext();
      }
    }
    if (event.key === "Escape") {
      onSearchChange("");
      searchInputRef.current?.blur();
    }
  };

  return (
    <HStack flex={1} gap={1} maxW="350px" minW="150px">
      <InputGroup
        endElement={
          searchQuery ? (
            <HStack gap={0.5}>
              <Text color="fg.muted" fontSize="xs" whiteSpace="nowrap">
                {totalMatches > 0
                  ? translate("logs.search.matchCount", {
                      current: currentMatchIndex + 1,
                      total: totalMatches,
                    })
                  : translate("logs.search.noMatches")}
              </Text>
              <IconButton
                aria-label="Previous match"
                disabled={totalMatches === 0}
                onClick={onSearchPrevious}
                size="2xs"
                variant="ghost"
              >
                <FiChevronUp />
              </IconButton>
              <IconButton
                aria-label="Next match"
                disabled={totalMatches === 0}
                onClick={onSearchNext}
                size="2xs"
                variant="ghost"
              >
                <FiChevronDown />
              </IconButton>
              <CloseButton onClick={() => onSearchChange("")} size="2xs" />
            </HStack>
          ) : undefined
        }
        startElement={<FiSearch />}
      >
        <Input
          data-testid="log-search-input"
          onChange={(event) => onSearchChange(event.target.value)}
          onKeyDown={handleSearchKeyDown}
          placeholder={translate("logs.search.placeholder")}
          ref={searchInputRef}
          size="sm"
          value={searchQuery}
        />
      </InputGroup>
    </HStack>
  );
};
