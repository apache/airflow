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
import { Box, Icon } from "@chakra-ui/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiClock } from "react-icons/fi";

import { Menu } from "src/components/ui";
import { useTimezone } from "src/context/timezone";

dayjs.extend(utc);
dayjs.extend(timezone);

export const TimezoneMenuItem = ({ onOpen }: { readonly onOpen: () => void }) => {
  const { t: translate } = useTranslation();
  const { selectedTimezone } = useTimezone();
  const [time, setTime] = useState(dayjs());

  useEffect(() => {
    const updateTime = () => {
      setTime(dayjs());
    };

    updateTime();

    const interval = setInterval(updateTime, 1000);

    return () => clearInterval(interval);
  }, [selectedTimezone]);

  return (
    <Menu.Item onClick={onOpen} value="timezone">
      <Icon as={FiClock} boxSize={4} />
      <Box flex="1">
        {translate("timezone")}: {dayjs(time).tz(selectedTimezone).format("HH:mm z (Z)")}
      </Box>
    </Menu.Item>
  );
};
