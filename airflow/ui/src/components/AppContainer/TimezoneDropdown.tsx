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

import React, { useState } from 'react';
import {
  Box,
  Button,
  Menu,
  MenuButton,
  MenuList,
  Tooltip,
} from '@chakra-ui/react';
import dayjs from 'dayjs';
import tz from 'dayjs/plugin/timezone';
import Select from 'components/Select';

import timezones from 'utils/timezones.json';
import { useTimezoneContext } from 'providers/TimezoneProvider';

dayjs.extend(tz);

interface Option { value: string, label: string }

const TimezoneDropdown: React.FC = () => {
  const { timezone, setTimezone } = useTimezoneContext();
  const [now, setNow] = useState(dayjs().tz());

  let currentTimezone: Option | null = null;

  const options = timezones.map(({ group, zones }) => ({
    label: group,
    options: zones.map(({ value, name }) => {
      if (value === timezone && !currentTimezone) currentTimezone = { value, label: name };
      return { value, label: name };
    }),
  }));

  const onChangeTimezone = (newTimezone: Option | null) => {
    if (newTimezone) {
      setTimezone(newTimezone.value);
      setNow(dayjs().tz(newTimezone.value));
    }
  };

  return (
    <Menu isLazy>
      <Tooltip label="Change time zone" hasArrow>
        <MenuButton as={Button} variant="ghost" mr="4">
          <Box
            as="time"
            dateTime={now.toString()}
            fontSize="md"
          >
            {now.format('h:mmA Z')}
          </Box>
        </MenuButton>
      </Tooltip>
      <MenuList placement="top-end">
        <Box px="3" pb="1">
          <Select
            autoFocus
            options={options}
            value={currentTimezone}
            onChange={onChangeTimezone}
          />
        </Box>
      </MenuList>
    </Menu>

  );
};

export default TimezoneDropdown;
