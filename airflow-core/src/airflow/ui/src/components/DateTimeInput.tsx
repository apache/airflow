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
import { Input, type InputProps } from "@chakra-ui/react";
import dayjs from "dayjs";
import tz from "dayjs/plugin/timezone";
import { forwardRef } from "react";

import { useTimezone } from "src/context/timezone";

dayjs.extend(tz);

// HTML datetime-local inputs require the ISO "T" separator, not a space.
const DATETIME_LOCAL_FORMAT = "YYYY-MM-DDTHH:mm:ss";

type Props = {
  readonly value: string;
} & InputProps;

export const DateTimeInput = forwardRef<HTMLInputElement, Props>(({ onChange, value, ...rest }, ref) => {
  const { selectedTimezone } = useTimezone();

  // Convert UTC value to the selected timezone for display in the native picker
  const displayValue =
    Boolean(value) && dayjs(value).isValid()
      ? dayjs(value).tz(selectedTimezone).format(DATETIME_LOCAL_FORMAT)
      : "";

  return (
    <Input
      onChange={(event) =>
        onChange?.({
          ...event,
          target: {
            ...event.target,
            value: dayjs(event.target.value).isValid()
              ? dayjs.tz(event.target.value, selectedTimezone).toISOString() // UI Timezone -> Utc -> yyyy-mm-ddThh:mm
              : "",
          },
        })
      }
      ref={ref}
      type="datetime-local"
      value={displayValue}
      {...rest}
    />
  );
});
