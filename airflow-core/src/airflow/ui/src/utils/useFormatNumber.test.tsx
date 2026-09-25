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

import { useFormatNumber } from "./useFormatNumber";

const NumberProbe = () => <span data-testid="number">{useFormatNumber()(1234)}</span>;

describe("useFormatNumber", () => {
  beforeAll(async () => {
    await i18n.use(initReactI18next).init({ fallbackLng: "en", lng: "en", resources: { de: {}, en: {} } });
  });

  afterEach(async () => {
    await act(async () => {
      await i18n.changeLanguage("en");
    });
  });

  it("groups digits for the active locale", async () => {
    render(<NumberProbe />);
    expect(screen.getByTestId("number")).toHaveTextContent("1,234");

    await act(async () => {
      await i18n.changeLanguage("de");
    });

    expect(screen.getByTestId("number")).toHaveTextContent("1.234");
  });

  it("falls back to the default locale instead of throwing on a malformed tag", async () => {
    // Plugins can contribute a UI language served via /static/i18n/languages.json without BCP-47
    // validation (e.g. "pt_BR"), which Intl.NumberFormat rejects with a RangeError.
    await act(async () => {
      await i18n.changeLanguage("pt_BR");
    });

    expect(() => render(<NumberProbe />)).not.toThrow();
    expect(screen.getByTestId("number")).toHaveTextContent("1,234");
  });
});
