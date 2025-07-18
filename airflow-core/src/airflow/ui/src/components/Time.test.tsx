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
import { render, screen } from "@testing-library/react";
import dayjs from "dayjs";
import { describe, it, expect, vi } from "vitest";

import { TimezoneContext } from "src/context/timezone";
import { Wrapper } from "src/utils/Wrapper";

import Time, { defaultFormat, defaultFormatWithTZ } from "./Time";

describe("Test Time and TimezoneProvider", () => {
  it("Displays a UTC time correctly", () => {
    const now = new Date();

    render(
      <TimezoneContext.Provider value={{ selectedTimezone: "UTC", setSelectedTimezone: vi.fn() }}>
        <Time datetime={now.toISOString()} />
      </TimezoneContext.Provider>,
      {
        wrapper: Wrapper,
      },
    );

    const utcTime = screen.getByText(dayjs.utc(now).format(defaultFormat));

    expect(utcTime).toBeDefined();
    expect(utcTime.title).toBeFalsy();
  });

  it("Displays a set timezone, includes UTC date in title", () => {
    const now = new Date();
    const tz = "US/Samoa";

    render(
      <TimezoneContext.Provider value={{ selectedTimezone: tz, setSelectedTimezone: vi.fn() }}>
        <Time datetime={now.toISOString()} />
      </TimezoneContext.Provider>,
      {
        wrapper: Wrapper,
      },
    );

    const nowTime = dayjs(now);
    const samoaTime = screen.getByText(nowTime.tz(tz).format(defaultFormat));

    expect(samoaTime).toBeDefined();
    expect(samoaTime.title).toEqual(nowTime.tz("UTC").format(defaultFormatWithTZ));
  });
});
