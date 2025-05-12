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
import { Box, Field, Text, VStack } from "@chakra-ui/react";
import { Select, type SingleValue } from "chakra-react-select";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import React, { useMemo } from "react";

import { useTimezone } from "src/context/timezone";
import type { Option as TimezoneOption } from "src/utils/option";

dayjs.extend(utc);
dayjs.extend(timezone);

const TimezoneSelector: React.FC = () => {
  const { selectedTimezone, setSelectedTimezone } = useTimezone();
  const timezones = useMemo<Array<string>>(() => {
    const tzList = Intl.supportedValuesOf("timeZone");
    const guessedTz = dayjs.tz.guess();
    const uniqueTimezones = new Set(["UTC", ...(guessedTz ? [guessedTz] : []), ...tzList]);

    return [...uniqueTimezones];
  }, []);

  const options = useMemo<Array<TimezoneOption>>(
    () =>
      timezones.map((tz) => ({
        label: tz === "UTC" ? "UTC (Coordinated Universal Time)" : tz,
        value: tz,
      })),
    [timezones],
  );

  const handleTimezoneChange = (selectedOption: SingleValue<TimezoneOption>) => {
    if (selectedOption) {
      setSelectedTimezone(selectedOption.value);
    }
  };

  const currentTime = dayjs().tz(selectedTimezone).format("YYYY-MM-DD HH:mm:ss");

  return (
    <VStack align="stretch" gap={6}>
      <Field.Root>
        <Select<TimezoneOption>
          onChange={handleTimezoneChange}
          options={options}
          placeholder="Select a timezone"
          value={options.find((option) => option.value === selectedTimezone)}
        />
      </Field.Root>
      <Box borderRadius="md" boxShadow="sm" display="flex" flexDirection="column" gap={2} p={6}>
        <Text fontSize="lg" fontWeight="bold">
          Current time in {selectedTimezone}:
        </Text>
        <Text fontSize="2xl">{currentTime}</Text>
      </Box>
    </VStack>
  );
};

export default TimezoneSelector;
