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

// Strings with an explicit timezone (`Z` or `+09:00`) are parsed as their
// absolute instant. Strings without one are treated as being in the selected
// Airflow UI timezone — consistent between manual input and paste.
const parseInput = (raw: string, timezone: string) => {
  const hasExplicitTz = /(?:[Zz]|[+-]\d{2}:?\d{2})$/u.test(raw);
  const parsed = hasExplicitTz ? dayjs(raw) : dayjs.tz(raw, timezone);

  return parsed.isValid() ? parsed : undefined;
};

type Props = {
  readonly value: string;
} & InputProps;

export const DateTimeInput = forwardRef<HTMLInputElement, Props>(({ onChange, value, ...rest }, ref) => {
  const { selectedTimezone } = useTimezone();
  const [displayDate, setDisplayDate] = useState(value);

  const emit = (event: ChangeEvent<HTMLInputElement> | ClipboardEvent<HTMLInputElement>, utc: string) => {
    onChange?.({
      ...event,
      target: { ...event.currentTarget, value: utc },
    });
  };

  const onDateChange = (event: ChangeEvent<HTMLInputElement>) => {
    const parsed = parseInput(event.target.value, selectedTimezone);

    // Set display value via UTC -> local to avoid year mismatch for years
    // before 1000 (dayjs/issues/1237).
    setDisplayDate(parsed ? parsed.tz(selectedTimezone).format(DEFAULT_DATETIME_FORMAT) : "");
    emit(event, parsed ? parsed.toISOString() : "");
  };

  const debouncedOnDateChange = useDebouncedCallback(
    (event: ChangeEvent<HTMLInputElement>) => onDateChange(event),
    debounceDelay,
  );

  const onPaste = (event: ClipboardEvent<HTMLInputElement>) => {
    const parsed = parseInput(event.clipboardData.getData("text").trim(), selectedTimezone);

    if (!parsed) {
      return;
    }

    event.preventDefault();
    // Drop any debounced call queued by prior typing so it cannot fire after
    // this paste and trigger a redundant onChange on the parent form.
    debouncedOnDateChange.cancel();
    // datetime-local input requires YYYY-MM-DDTHH:mm format in the selected
    // Airflow UI timezone (not the browser's local timezone).
    setDisplayDate(parsed.tz(selectedTimezone).format("YYYY-MM-DDTHH:mm"));
    emit(event, parsed.toISOString());
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
