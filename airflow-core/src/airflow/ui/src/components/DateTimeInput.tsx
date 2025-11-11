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

const CustomInput = forwardRef<HTMLInputElement, InputProps>(({ onClick, value, ...rest }, ref) => (
  <Input {...rest} onClick={onClick} ref={ref} value={value} w="full" />
));

CustomInput.displayName = "CustomInput";

export const DateTimeInput = forwardRef<HTMLInputElement, InputProps>(({ onChange, value, ...rest }, ref) => {
  const { selectedTimezone } = useTimezone();

  const selected =
    Boolean(value) && typeof value === "string" && dayjs(value).isValid() ? dayjs(value).toDate() : undefined;

  const handleChange = (date: Date | null) => {
    if (!onChange) {
      return;
    }

    const localDateTimeString = date ? dayjs(date).format("YYYY-MM-DDTHH:mm:ss") : "";
    const newValue = localDateTimeString
      ? dayjs(localDateTimeString).tz(selectedTimezone, true).toISOString()
      : "";

    const event = {
      target: {
        value: newValue,
      },
    } as React.ChangeEvent<HTMLInputElement>;

    onChange(event);
  };

  return (
    <DatePicker
      customInput={<CustomInput {...rest} ref={ref} />}
      dateFormat="yyyy-MM-dd HH:mm"
      onChange={handleChange}
      popperPlacement="bottom-start"
      selected={selected}
      showTimeSelect
      timeFormat="HH:mm"
    />
  );
});
