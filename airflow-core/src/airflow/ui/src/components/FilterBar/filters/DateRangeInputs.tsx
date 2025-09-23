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
import { Box, HStack, IconButton, Input, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { MdClose } from "react-icons/md";

import type { DateRangeEditingState } from "src/hooks/useDateRangeFilter";

type DateRangeInputsProps = {
  readonly editingState: DateRangeEditingState;
  readonly endDateValue?: dayjs.Dayjs;
  readonly onChange: (field: "end" | "start") => (event: React.ChangeEvent<HTMLInputElement>) => void;
  readonly onClearEnd: () => void;
  readonly onClearStart: () => void;
  readonly setEditingState: React.Dispatch<React.SetStateAction<DateRangeEditingState>>;
  readonly setSelectionTarget: (target: "end" | "start") => void;
  readonly startDateValue?: dayjs.Dayjs;
};

export const DateRangeInputs = ({
  editingState,
  endDateValue,
  onChange,
  onClearEnd,
  onClearStart,
  setEditingState,
  setSelectionTarget,
  startDateValue,
}: DateRangeInputsProps) => {
  const { t: translate } = useTranslation(["common"]);

  return (
    <HStack gap={2} w="full">
      <Box position="relative" w="140px">
        <Text color="gray.600" fontSize="xs" mb={0.5}>
          {translate("common:table.from")}
        </Text>
        <Input
          _focus={{ borderColor: "blue.500" }}
          borderColor={editingState.selectionTarget === "start" ? "blue.500" : "gray.300"}
          fontSize="sm"
          fontWeight="medium"
          onBlur={() => {
            if (
              startDateValue &&
              editingState.inputs.start &&
              !dayjs(editingState.inputs.start, "YYYY/MM/DD", true).isValid()
            ) {
              setEditingState((prev) => ({
                ...prev,
                inputs: { ...prev.inputs, start: startDateValue.format("YYYY/MM/DD") },
              }));
            }
          }}
          onChange={onChange("start")}
          onFocus={() => setSelectionTarget("start")}
          placeholder="YYYY/MM/DD"
          value={editingState.inputs.start}
        />
        {Boolean(editingState.inputs.start) ? (
          <IconButton
            aria-label="Clear start date"
            onClick={onClearStart}
            position="absolute"
            right={1}
            size="2xs"
            top="50%"
            variant="ghost"
          >
            <MdClose size={12} />
          </IconButton>
        ) : undefined}
      </Box>

      <Box position="relative" w="140px">
        <Text color="gray.600" fontSize="xs" mb={0.5}>
          {translate("common:table.to")}
        </Text>
        <Input
          _focus={{ borderColor: "blue.500" }}
          borderColor={editingState.selectionTarget === "end" ? "blue.500" : "gray.300"}
          fontSize="sm"
          fontWeight="medium"
          onBlur={() => {
            if (
              endDateValue &&
              editingState.inputs.end &&
              !dayjs(editingState.inputs.end, "YYYY/MM/DD", true).isValid()
            ) {
              setEditingState((prev) => ({
                ...prev,
                inputs: { ...prev.inputs, end: endDateValue.format("YYYY/MM/DD") },
              }));
            }
          }}
          onChange={onChange("end")}
          onFocus={() => setSelectionTarget("end")}
          placeholder="YYYY/MM/DD"
          value={editingState.inputs.end}
        />
        {Boolean(editingState.inputs.end) ? (
          <IconButton
            aria-label="Clear end date"
            onClick={onClearEnd}
            position="absolute"
            right={1}
            size="2xs"
            top="50%"
            variant="ghost"
          >
            <MdClose size={12} />
          </IconButton>
        ) : undefined}
      </Box>
    </HStack>
  );
};
