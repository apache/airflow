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
import { Box, IconButton, Input, Text } from "@chakra-ui/react";
import React from "react";
import { MdClose } from "react-icons/md";

import type { ValidationError } from "src/hooks/useDateRangeFilter";

type DateInputProps = {
  readonly field: "end" | "start";
  readonly getBorderColor: (field: ValidationError["field"]) => string;
  readonly getFieldError: (field: ValidationError["field"]) => ValidationError | undefined;
  readonly handleInputChange: (
    field: "end" | "start",
    inputType: "date" | "time",
  ) => (event: React.ChangeEvent<HTMLInputElement>) => void;
  readonly inputType: "date" | "time";
  readonly inputValue: string;
  readonly label: string;
  readonly onClear: () => void;
  readonly onDateBlur?: () => void;
  readonly onFocus?: () => void;
  readonly placeholder: string;
};

export const DateInput = ({
  field,
  getBorderColor,
  getFieldError,
  handleInputChange,
  inputType,
  inputValue,
  label,
  onClear,
  onDateBlur,
  onFocus,
  placeholder,
}: DateInputProps) => {
  const fieldName = inputType === "date" ? field : (`${field}Time` as const);

  return (
    <Box flex="1">
      <Text color="fg.muted" fontSize="xs" mb={0.25}>
        {label}
      </Text>
      <Box position="relative">
        <Input
          _focus={{ borderColor: "brand.focusRing" }}
          borderColor={getBorderColor(fieldName)}
          fontSize="sm"
          fontWeight="medium"
          onBlur={onDateBlur}
          onChange={handleInputChange(field, inputType)}
          onFocus={onFocus}
          placeholder={placeholder}
          value={inputValue}
          w="full"
        />
        {Boolean(inputValue) && (
          <IconButton
            aria-label={`Clear ${field} ${inputType}`}
            onClick={(event) => {
              event.stopPropagation();
              onClear();
            }}
            position="absolute"
            right={1}
            size="2xs"
            top="50%"
            transform="translateY(-50%)"
            variant="ghost"
          >
            <MdClose size={12} />
          </IconButton>
        )}
      </Box>
      <Box alignItems="flex-start" display="flex" minH="12px">
        {getFieldError(fieldName) && (
          <Text color="danger.fg" fontSize="xs" mt={0.5}>
            {getFieldError(fieldName)?.message}
          </Text>
        )}
      </Box>
    </Box>
  );
};
