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
import { HStack, Text, type SelectValueChangeDetails } from "@chakra-ui/react";
import { createListCollection, type ListCollection } from "@chakra-ui/react/collection";
import dayjs from "dayjs";
import { FiCalendar } from "react-icons/fi";

import Time from "src/components/Time";
import { Select } from "src/components/ui";

type Props = {
  readonly defaultValue: string;
  readonly endDate: string;
  readonly setEndDate: (startDate: string) => void;
  readonly setStartDate: (startDate: string) => void;
  readonly startDate: string;
  readonly timeOptions?: ListCollection<{ label: string; value: string }>;
};

const defaultTimeOptions = createListCollection({
  items: [
    { label: "Last 1 hour", value: "1" },
    { label: "Last 12 hours", value: "12" },
    { label: "Last 24 hours", value: "24" },
    { label: "Past week", value: "168" },
  ],
});

const TimeRangeSelector = ({
  defaultValue,
  endDate,
  setEndDate,
  setStartDate,
  startDate,
  timeOptions = defaultTimeOptions,
}: Props) => {
  const handleTimeChange = ({ value }: SelectValueChangeDetails<Array<string>>) => {
    const cnow = dayjs();

    setStartDate(cnow.subtract(Number(value[0]), "hour").toISOString());
    setEndDate(cnow.toISOString());
  };

  return (
    <HStack flexWrap="wrap">
      <FiCalendar />
      <Select.Root
        collection={timeOptions}
        data-testid="filter-duration"
        defaultValue={[defaultValue]}
        onValueChange={handleTimeChange}
        width="200px"
      >
        <Select.Trigger>
          <Select.ValueText placeholder="Duration" />
        </Select.Trigger>
        <Select.Content>
          {timeOptions.items.map((option) => (
            <Select.Item item={option} key={option.value}>
              {option.label}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
      <Text>
        <Time datetime={startDate} /> - <Time datetime={endDate} />
      </Text>
    </HStack>
  );
};

export default TimeRangeSelector;
