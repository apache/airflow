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
import { Box, HStack } from "@chakra-ui/react";
import type { RefObject } from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { MdClose } from "react-icons/md";

import { IconButton } from "../ui";
import { getDefaultFilterIcon } from "./defaultIcons";
import type { FilterState } from "./types";
import { isEmptyFilterValue } from "./utils";

export type FilterPillInputProps = {
  onBlur: () => void;
  onFocus: () => void;
  onKeyDown: (event: React.KeyboardEvent) => void;
  ref: RefObject<HTMLInputElement | null>;
};

type FilterPillProps = {
  readonly displayValue: React.ReactNode | string;
  readonly filter: FilterState;
  readonly hasValue: boolean;
  // Replaces entering edit mode when the chip body is clicked, for pills that have
  // nothing to edit (boolean).
  readonly onClick?: () => void;
  readonly onRemove: () => void;
  readonly renderInput: (props: FilterPillInputProps) => React.ReactNode;
};

export const FilterPill = ({
  displayValue,
  filter,
  hasValue,
  onClick,
  onRemove,
  renderInput,
}: FilterPillProps) => {
  const { t: translate } = useTranslation(["common"]);
  const isEmpty = isEmptyFilterValue(filter.value);
  const [isEditing, setIsEditing] = useState(isEmpty);
  const inputRef = useRef<HTMLInputElement>(null);
  const blurTimeoutRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const handlePillClick = () => (onClick === undefined ? setIsEditing(true) : onClick());

  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "Enter" || event.key === "Escape") {
      setIsEditing(false);
    }
  };

  const handleBlur = () => {
    blurTimeoutRef.current = setTimeout(() => setIsEditing(false), 150);
  };

  const handleFocus = () => {
    if (blurTimeoutRef.current !== undefined) {
      clearTimeout(blurTimeoutRef.current);
      blurTimeoutRef.current = undefined;
    }
  };

  useEffect(() => {
    if (isEditing && inputRef.current) {
      const input = inputRef.current;
      const focusInput = () => {
        input.focus();
        try {
          input.select();
        } catch {
          // NumberInputField doesn't support select()
        }
      };

      requestAnimationFrame(focusInput);
    }
  }, [isEditing]);

  useEffect(
    () => () => {
      if (blurTimeoutRef.current !== undefined) {
        clearTimeout(blurTimeoutRef.current);
      }
    },
    [],
  );

  if (isEditing) {
    return renderInput({
      onBlur: handleBlur,
      onFocus: handleFocus,
      onKeyDown: handleKeyDown,
      ref: inputRef,
    });
  }

  return (
    <Box
      _hover={{ bg: "colorPalette.subtle" }}
      as="button"
      bg={hasValue ? "blue.muted" : "gray.muted"}
      borderRadius="full"
      color="colorPalette.fg"
      colorPalette={hasValue ? "blue" : "gray"}
      cursor="pointer"
      data-testid={`${filter.config.key}-pill`}
      display="flex"
      fontSize="sm"
      fontWeight="medium"
      h="10"
      onClick={handlePillClick}
      px={4}
    >
      <HStack align="center">
        {filter.config.icon ?? getDefaultFilterIcon(filter.config.type)}
        <Box alignItems="center" display="flex" flex="1" gap={2} px={2}>
          {filter.config.label}
          {displayValue === undefined || displayValue === "" ? undefined : <>: {displayValue}</>}
        </Box>
        <IconButton
          aria-label={`Remove ${filter.config.label} filter`}
          borderRadius="full"
          label={translate("common:filters.removeFilter")}
          mr={-3}
          onClick={(event) => {
            event.stopPropagation();
            onRemove();
          }}
          size="xs"
        >
          <MdClose size={16} />
        </IconButton>
      </HStack>
    </Box>
  );
};
