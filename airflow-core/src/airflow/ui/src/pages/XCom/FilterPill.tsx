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
import { Box, Button, HStack, IconButton, Input } from "@chakra-ui/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { MdClose } from "react-icons/md";

export type FilterType = "dag_id" | "key" | "run_id" | "task_id";

type FilterPillProps = {
  readonly filterType: FilterType;
  readonly label: string;
  readonly onRemove: () => void;
  readonly onValueChange: (value: string) => void;
  readonly value: string;
};

export const FilterPill = ({ label, onRemove, onValueChange, value }: FilterPillProps) => {
  const [isEditing, setIsEditing] = useState(value === "");
  const [inputValue, setInputValue] = useState(value);
  const inputRef = useRef<HTMLInputElement>(null);

  const handlePillClick = useCallback(() => {
    setIsEditing(true);
  }, []);

  const handleInputKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (event.key === "Enter") {
        setIsEditing(false);
        onValueChange(inputValue);
      } else if (event.key === "Escape") {
        setInputValue(value);
        setIsEditing(false);
      }
    },
    [inputValue, onValueChange, value],
  );

  const handleInputBlur = useCallback(() => {
    setIsEditing(false);
    onValueChange(inputValue);
  }, [inputValue, onValueChange]);

  const handleInputChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    setInputValue(event.target.value);
  }, []);

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus();
    }
  }, [isEditing]);

  useEffect(() => {
    setInputValue(value);
  }, [value]);

  if (isEditing) {
    return (
      <Input
        borderRadius="full"
        onBlur={handleInputBlur}
        onChange={handleInputChange}
        onKeyDown={handleInputKeyDown}
        placeholder={`Enter ${label.toLowerCase()}`}
        ref={inputRef}
        size="sm"
        value={inputValue}
        width="220px"
      />
    );
  }

  return (
    <Button
      _hover={{ bg: "colorPalette.subtle" }}
      bg={value ? "blue.muted" : "gray.muted"}
      borderRadius="full"
      color="colorPalette.fg"
      colorPalette={value ? "blue" : "gray"}
      cursor="pointer"
      onClick={handlePillClick}
      size="sm"
    >
      <HStack align="center" gap={0}>
        <Box flex="1" fontSize="sm" fontWeight="medium" px={3} py={2}>
          {label}: {value || ""}
        </Box>

        <IconButton
          _hover={{
            bg: "red.100",
            color: "red.600",
          }}
          aria-label={`Remove ${label} filter`}
          bg="transparent"
          color="gray.400"
          mr={1}
          onClick={(event) => {
            event.stopPropagation();
            onRemove();
          }}
          rounded="full"
          size="xs"
          transition="all 0.2s"
          variant="ghost"
        >
          <MdClose size={12} />
        </IconButton>
      </HStack>
    </Button>
  );
};
