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
import dayjs from "dayjs";
import advancedFormat from "dayjs/plugin/advancedFormat";
import tz from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";

import { useTimezone } from "src/context/timezone";

export const defaultFormat = "YYYY-MM-DD, HH:mm:ss";
export const defaultFormatWithTZ = `${defaultFormat} z`;
export const defaultTZFormat = "z (Z)";

dayjs.extend(utc);
dayjs.extend(tz);
dayjs.extend(advancedFormat);

type Props = {
  readonly datetime?: string | null;
  readonly format?: string;
};

const Time = ({ datetime, format = defaultFormat }: Props) => {
  const { selectedTimezone } = useTimezone();
  const time = dayjs(datetime);

  if (datetime === null || datetime === undefined || !time.isValid()) {
    return undefined;
  }

  const formattedTime = time.tz(selectedTimezone).format(format);
  const utcTime = time.tz("UTC").format(defaultFormatWithTZ);

  return (
    <time
      dateTime={datetime}
      // show title if date is not UTC
      title={selectedTimezone.toUpperCase() === "UTC" ? undefined : utcTime}
    >
      {formattedTime}
    </time>
  );
};

export default Time;
