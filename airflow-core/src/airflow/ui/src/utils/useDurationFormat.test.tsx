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
import { render, screen, act } from "@testing-library/react";
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import { describe, it, expect, beforeAll, afterEach } from "vitest";

import { useDurationFormat } from "./useDurationFormat";

// A component that shows a duration and nothing else. Before this hook existed the formatters read
// the language straight off the i18next singleton, so a component like this — with no reason of its
// own to subscribe to `languageChanged` — never re-rendered and kept the previous locale forever.
const DurationOnly = () => {
  const { renderDuration } = useDurationFormat();

  return <span data-testid="duration">{renderDuration(3725.4)}</span>;
};

const LocaleProbe = () => <span data-testid="locale">{useDurationFormat().locale}</span>;

const expected = (locale: string) => {
  const unit = (unitName: Intl.NumberFormatOptions["unit"], value: number) =>
    new Intl.NumberFormat(locale, { style: "unit", unit: unitName, unitDisplay: "narrow" }).format(value);

  return new Intl.ListFormat(locale, { style: "narrow", type: "unit" }).format([
    unit("hour", 1),
    unit("minute", 2),
  ]);
};

describe("useDurationFormat", () => {
  beforeAll(async () => {
    await i18n.use(initReactI18next).init({ fallbackLng: "en", lng: "en", resources: { de: {}, en: {} } });
  });

  afterEach(async () => {
    await act(async () => {
      await i18n.changeLanguage("en");
    });
  });

  it("re-formats a duration when the language changes, without any other subscription", async () => {
    render(<DurationOnly />);
    expect(screen.getByTestId("duration")).toHaveTextContent(expected("en"));

    await act(async () => {
      await i18n.changeLanguage("de");
    });

    expect(screen.getByTestId("duration")).toHaveTextContent(expected("de"));
    expect(expected("de")).not.toBe(expected("en"));
  });

  it("exposes the active locale so callers can key their own memos on it", async () => {
    render(<LocaleProbe />);
    expect(screen.getByTestId("locale")).toHaveTextContent("en");

    await act(async () => {
      await i18n.changeLanguage("de");
    });

    expect(screen.getByTestId("locale")).toHaveTextContent("de");
  });
});
