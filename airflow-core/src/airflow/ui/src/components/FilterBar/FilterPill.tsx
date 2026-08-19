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
import type { RefObject, KeyboardEvent, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { MdClose } from "react-icons/md";

import { IconButton } from "../ui";
import { getDefaultFilterIcon } from "./defaultIcons";
import type { FilterState } from "./types";
import { isEmptyFilterValue } from "./utils";

/** Spread straight onto the editor's element, so everything here has to be DOM-safe. */
export type FilterPillInputProps = {
  onBlur: () => void;
  onFocus: () => void;
  onKeyDown: (event: KeyboardEvent) => void;
  ref: RefObject<HTMLInputElement | null>;
};

export type FilterPillControls = {
  /**
   * Leave edit mode now. For editors that dismiss themselves — anything with a popover or a
   * portalled menu — closing returns focus inside the pill, so no blur fires and the pill would
   * otherwise stay open forever. Drops the filter when it still has no value.
   *
   * Passed separately from ``FilterPillInputProps`` because that object gets spread onto a DOM
   * element, which React would warn about for a handler it does not know.
   */
  onRequestClose: () => void;
};

type FilterPillProps = {
  readonly displayValue: ReactNode | string;
  readonly filter: FilterState;
  readonly hasValue: boolean;
  // Replaces entering edit mode when the chip body is clicked, for pills that have
  // nothing to edit (boolean).
  readonly onClick?: () => void;
  readonly onRemove: () => void;
  readonly renderInput: (props: FilterPillInputProps, controls: FilterPillControls) => ReactNode;
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
  // Read inside the blur timeout, which would otherwise close over a stale value.
  const valueRef = useRef(filter.value);

  valueRef.current = filter.value;

  const handlePillClick = () => (onClick === undefined ? setIsEditing(true) : onClick());

  // Leaving the editor without choosing anything drops the filter rather than parking a
  // valueless pill on the bar, which reads as active but filters nothing.
  const stopEditing = () => {
    if (isEmptyFilterValue(valueRef.current)) {
      onRemove();
    } else {
      setIsEditing(false);
    }
  };

  const handleKeyDown = (event: KeyboardEvent) => {
    if (event.key === "Enter" || event.key === "Escape") {
      stopEditing();
    }
  };

  const handleBlur = () => {
    blurTimeoutRef.current = setTimeout(stopEditing, 150);
  };

  const handleFocus = () => {
    if (blurTimeoutRef.current !== undefined) {
      clearTimeout(blurTimeoutRef.current);
      blurTimeoutRef.current = undefined;
    }
  };

  const handleRequestClose = () => {
    handleFocus(); // Cancels any blur already in flight so the close only happens once.
    stopEditing();
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
    return renderInput(
      {
        onBlur: handleBlur,
        onFocus: handleFocus,
        onKeyDown: handleKeyDown,
        ref: inputRef,
      },
      { onRequestClose: handleRequestClose },
    );
  }

  return (
    <Box
      _hover={{ bg: "colorPalette.subtle" }}
      alignItems="center"
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
      gap={2}
      h="9"
      onClick={handlePillClick}
      pl={3}
      pr={1}
    >
      {filter.config.icon ?? getDefaultFilterIcon(filter.config.type)}
      <Box alignItems="center" display="flex" flex="1" textWrap="nowrap">
        {filter.config.label}
        {displayValue === undefined || displayValue === "" ? undefined : <strong>: {displayValue}</strong>}
      </Box>
      <IconButton
        aria-label={`Remove ${filter.config.label} filter`}
        borderRadius="full"
        label={translate("common:filters.removeFilter")}
        onClick={(event) => {
          event.stopPropagation();
          onRemove();
        }}
        size="xs"
      >
        <MdClose size={16} />
      </IconButton>
    </Box>
  );
};
