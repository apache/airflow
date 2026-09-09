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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { createInstance } from "i18next";
import { I18nextProvider, initReactI18next } from "react-i18next";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import LanguageSelector from "./LanguageSelector";

// The selector lists plugin languages from the /static/i18n/languages.json manifest; stub the
// fetch so tests control which plugin languages are offered.
const renderWithLanguages = async (pluginLanguages: Array<string>, lng: string) => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ json: () => Promise.resolve({ languages: pluginLanguages }), ok: true }),
  );

  const instance = createInstance();

  await instance.use(initReactI18next).init({
    fallbackLng: false,
    lng,
    react: { useSuspense: false },
    resources: {},
    supportedLngs: [...new Set(["en", lng, ...pluginLanguages])],
  });

  render(
    <I18nextProvider i18n={instance}>
      <LanguageSelector />
    </I18nextProvider>,
    { wrapper: Wrapper },
  );
};

describe("LanguageSelector", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("labels a built-in language with its curated name", async () => {
    await renderWithLanguages([], "fr");

    expect(await screen.findByText(/Français \(fr\)/u)).toBeInTheDocument();
  });

  it("lists a plugin-contributed language with the browser's language name", async () => {
    const expected = new Intl.DisplayNames(["eo"], { type: "language" }).of("eo");

    await renderWithLanguages(["eo"], "eo");

    expect(await screen.findByText(new RegExp(`${expected ?? "eo"} \\(eo\\)`, "u"))).toBeInTheDocument();
  });
});
