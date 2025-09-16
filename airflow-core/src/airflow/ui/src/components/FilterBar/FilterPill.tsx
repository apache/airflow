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
import React from "react";
import { useEffect, useRef, useState } from "react";
import { MdClose } from "react-icons/md";

import { getDefaultFilterIcon } from "./defaultIcons";
import type { FilterState, FilterValue } from "./types";

type FilterPillProps = {
  readonly children: React.ReactNode;
  readonly displayValue: React.ReactNode | string;
  readonly filter: FilterState;
  readonly hasValue: boolean;
  readonly onChange: (value: FilterValue) => void;
  readonly onRemove: () => void;
};

export const FilterPill = ({
  children,
  displayValue,
  filter,
  hasValue,
  onChange,
  onRemove,
}: FilterPillProps) => {
  const isEmpty = filter.value === null || filter.value === undefined || String(filter.value).trim() === "";
  const [isEditing, setIsEditing] = useState(isEmpty);
  const inputRef = useRef<HTMLInputElement>(null);
  const blurTimeoutRef = useRef<NodeJS.Timeout | undefined>(undefined);

  const handlePillClick = () => setIsEditing(true);

  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "Enter" || event.key === "Escape") {
      setIsEditing(false);
    }
  };

  const handleBlur = () => {
    blurTimeoutRef.current = setTimeout(() => setIsEditing(false), 150);
  };

  const handleFocus = () => {
    if (blurTimeoutRef.current) {
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
      if (blurTimeoutRef.current) {
        clearTimeout(blurTimeoutRef.current);
      }
    },
    [],
  );

  const childrenWithProps = React.Children.map(children, (child) => {
    if (React.isValidElement(child)) {
      return React.cloneElement(child, {
        onBlur: handleBlur,
        onChange,
        onFocus: handleFocus,
        onKeyDown: handleKeyDown,
        ref: inputRef,
        ...child.props,
      } as Record<string, unknown>);
    }

    return child;
  });

  if (isEditing) {
    return childrenWithProps;
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
      display="flex"
      fontSize="sm"
      fontWeight="medium"
      h="10"
      onClick={handlePillClick}
      px={4}
    >
      <HStack align="center" gap={1}>
        {filter.config.icon ?? getDefaultFilterIcon(filter.config.type)}
        <Box alignItems="center" display="flex" flex="1" gap={2} px={2}>
          {filter.config.label}: {displayValue}
        </Box>

        <Box
          _hover={{
            bg: "gray.100",
            color: "gray.600",
          }}
          alignItems="center"
          aria-label={`Remove ${filter.config.label} filter`}
          bg="transparent"
          borderRadius="full"
          color="gray.400"
          cursor="pointer"
          display="flex"
          h={6}
          justifyContent="center"
          mr={1}
          onClick={(event) => {
            event.stopPropagation();
            onRemove();
          }}
          transition="all 0.2s"
          w={6}
        >
          <MdClose size={16} />
        </Box>
      </HStack>
    </Box>
  );
};
