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

type Props = {
  readonly value: string;
} & InputProps;

export const DateTimeInput = forwardRef<HTMLInputElement, Props>(({ onChange, value, ...rest }, ref) => {
  const { selectedTimezone } = useTimezone();

  // Make the value timezone-aware
  const date = dayjs(value).tz(selectedTimezone).format("YYYY-MM-DDTHH:mm:ss.SSS");

  return (
    <Input
      onChange={(event) =>
        onChange?.({
          ...event,
          target: {
            ...event.target,
            // Return a timezone-aware ISO string
            value: dayjs(event.target.value).isValid()
              ? dayjs(event.target.value).tz(selectedTimezone, true).toISOString()
              : "",
          },
        })
      }
      ref={ref}
      type="datetime-local"
      value={date}
      {...rest}
    />
  );
});
