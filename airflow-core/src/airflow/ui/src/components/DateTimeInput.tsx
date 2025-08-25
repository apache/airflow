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
import DatePicker from "react-datepicker";

import "react-datepicker/dist/react-datepicker.css";
import { useTimezone } from "src/context/timezone";

dayjs.extend(tz);

// We are replacing the native datetime-local input with react-datepicker for cross-browser compatibility.
// The component's public API (props) should remain the same to avoid breaking changes in parent components.
export const DateTimeInput = forwardRef<HTMLInputElement, InputProps>(
  ({ onChange, value, ...rest }, ref) => {
    const { selectedTimezone } = useTimezone();

    // The `value` from the form is a string (UTC ISO format). Convert to a Date object for DatePicker.
    // DatePicker will display this in the user's browser timezone.
    const selected =
      value && typeof value === "string" && dayjs(value).isValid()
        ? dayjs(value).toDate()
        : null;

    const handleChange = (date: Date | null) => {
      if (!onChange) {return;}

      // When a date is selected, `react-datepicker` provides a Date object.
      // This object represents the selected time in the user's browser timezone.
      // We need to convert this back to a UTC ISO string, but interpret the time as if it were in the `selectedTimezone`.
      // This preserves the user's intent. For example, if user selects 10:00, we treat it as 10:00 in the `selectedTimezone`.

      // 1. Format the date to a local time string, e.g., "2025-08-22T10:00:00"
      const localDateTimeString = date
        ? dayjs(date).format("YYYY-MM-DDTHH:mm:ss")
        : "";

      // 2. Interpret this local time string as being in the `selectedTimezone` and convert to a UTC ISO string.
      const newValue = localDateTimeString
        ? dayjs(localDateTimeString).tz(selectedTimezone, true).toISOString()
        : "";

      // 3. The parent component expects a ChangeEvent, so we simulate one.
      const event = {
        target: {
          value: newValue,
        },
      } as React.ChangeEvent<HTMLInputElement>;

      onChange(event);
    };

    // Use a custom input to apply Chakra styling and pass down props.
    const CustomInput = forwardRef<HTMLInputElement, { readonly value?: string }>(
      (props, customRef) => <Input {...rest} {...props} ref={customRef} />
    );

    CustomInput.displayName = "CustomInput";

    return (
      <DatePicker
        customInput={<CustomInput />}
        dateFormat="yyyy-MM-dd HH:mm"
        onChange={handleChange}
        popperPlacement="bottom-start"
        ref={ref}
        selected={selected}
        showTimeSelect
        timeFormat="HH:mm"
      />
    );
  }
);
