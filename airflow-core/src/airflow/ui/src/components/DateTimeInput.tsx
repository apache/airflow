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
import { forwardRef, type ChangeEvent, type ClipboardEvent, useState } from "react";
import { useDebouncedCallback } from "use-debounce";

import { useTimezone } from "src/context/timezone";
import { DEFAULT_DATETIME_FORMAT } from "src/utils/datetimeUtils";

dayjs.extend(tz);

const debounceDelay = 1000;

type Props = {
  readonly value: string;
} & InputProps;

export const DateTimeInput = forwardRef<HTMLInputElement, Props>(({ onChange, value, ...rest }, ref) => {
  const { selectedTimezone } = useTimezone();
  const [displayDate, setDisplayDate] = useState(value);

  const onDateChange = (event: ChangeEvent<HTMLInputElement>) => {
    const valid = dayjs(event.target.value).isValid();
    // UI Timezone -> Utc -> yyyy-mm-ddThh:mmZ
    const utc = valid ? dayjs.tz(event.target.value, selectedTimezone).toISOString() : "";
    const local = Boolean(utc) ? dayjs(utc).tz(selectedTimezone).format(DEFAULT_DATETIME_FORMAT) : "";

    // Set display value to be from utc to local to avoid year mismatch
    // As dayjs() parses years before 1000 incorrectly, see dayjs/issues/1237
    setDisplayDate(local);
    onChange?.({ ...event, target: { ...event.target, value: utc } });
  };

  const debouncedOnDateChange = useDebouncedCallback(
    (event: ChangeEvent<HTMLInputElement>) => onDateChange(event),
    debounceDelay,
  );

  const onPaste = (event: ClipboardEvent<HTMLInputElement>) => {
    const pasted = event.clipboardData.getData("text").trim();
    // Pasted strings with an explicit timezone (e.g. `Z` or `+09:00`) are parsed
    // as their absolute instant. Strings without one are treated as being in the
    // selected Airflow UI timezone — consistent with manual input.
    const hasExplicitTz = /(?:[Zz]|[+-]\d{2}:?\d{2})$/u.test(pasted);
    const parsed = hasExplicitTz ? dayjs(pasted) : dayjs.tz(pasted, selectedTimezone);

    if (!parsed.isValid()) {
      return;
    }

    event.preventDefault();
    // datetime-local input requires YYYY-MM-DDTHH:mm format in the selected
    // Airflow UI timezone (not the browser's local timezone).
    const localFormat = parsed.tz(selectedTimezone).format("YYYY-MM-DDTHH:mm");

    onDateChange({
      ...event,
      target: { ...event.currentTarget, value: localFormat },
    });
    // Override the display set by onDateChange (which uses DEFAULT_DATETIME_FORMAT)
    // so the datetime-local input keeps the YYYY-MM-DDTHH:mm format it requires.
    setDisplayDate(localFormat);
  };

  return (
    <Input
      data-testid="datetime-input"
      onChange={(event) => {
        setDisplayDate(dayjs(event.target.value).isValid() ? event.target.value : "");
        debouncedOnDateChange(event);
      }}
      onPaste={onPaste}
      ref={ref}
      type="datetime-local"
      value={displayDate}
      {...rest}
    />
  );
});
