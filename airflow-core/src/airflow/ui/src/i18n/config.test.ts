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
import { createInstance } from "i18next";
import { describe, expect, it } from "vitest";

import { convertDetectedLanguage, i18nBaseOptions } from "./config";

// getBestMatchFromCodes is the resolver i18next runs on the array the
// LanguageDetector returns. It is not part of i18next's public types
// (services.languageUtils is `any`), so we pin the one method we use.
type LanguageUtils = { getBestMatchFromCodes: (codes: ReadonlyArray<string>) => string };

// Resolve a language exactly as the running app does: the detector maps
// convertDetectedLanguage over navigator.languages (preserving order), then
// i18next picks the best match. Uses the production init options so the test
// guards the real config rather than a copy.
const resolveLanguage = async (navigatorLanguages: ReadonlyArray<string>): Promise<string> => {
  const instance = createInstance();

  await instance.init({ ...i18nBaseOptions, initImmediate: false, resources: {} });

  const languageUtils = instance.services.languageUtils as LanguageUtils;

  return languageUtils.getBestMatchFromCodes(navigatorLanguages.map(convertDetectedLanguage));
};

describe("i18n language resolution", () => {
  it.each([
    // The reported bug: real UK Chrome with Hindi added. Before the fix this
    // resolved to "hi" because "en-GB"/"en-US" are not exact members of
    // supportedLngs but "hi" is, so the first bare supported code won.
    { expected: "en", languages: ["en-GB", "hi-IN", "hi", "en-US", "en", "gu"] },
    { expected: "en", languages: ["en-GB", "hi"] },
    { expected: "en", languages: ["en-GB"] },
    // Browsers may emit non-canonical casing.
    { expected: "en", languages: ["EN-GB", "hi"] },
    // Other region-qualified base languages were affected the same way.
    { expected: "pt", languages: ["pt-BR", "en"] },
    { expected: "de", languages: ["de-AT", "de", "en"] },
    { expected: "fr", languages: ["fr-FR", "fr", "en"] },
  ])(
    "resolves $languages to $expected (region variant of a base language)",
    async ({ expected, languages }) => {
      expect(await resolveLanguage(languages)).toBe(expected);
    },
  );

  it.each([
    // Region-specific supported locales must stay intact even when English
    // follows them -- this is what nonExplicitSupportedLngs/load:"languageOnly"
    // would break by collapsing "zh-CN"/"zh-TW" to the unsupported "zh".
    { expected: "zh-CN", languages: ["zh-CN"] },
    { expected: "zh-CN", languages: ["zh-CN", "zh", "en"] },
    { expected: "zh-CN", languages: ["zh-CN", "en-US"] },
    { expected: "zh-TW", languages: ["zh-TW", "zh", "en"] },
    { expected: "zh-TW", languages: ["zh-TW", "en"] },
    // Traditional Chinese via script/region subtags (e.g. macOS Safari) must
    // resolve to zh-TW, not be collapsed to Simplified by a generic "zh-*" match.
    { expected: "zh-TW", languages: ["zh-Hant-TW", "en"] },
    { expected: "zh-TW", languages: ["zh-Hant", "en"] },
    { expected: "zh-TW", languages: ["zh-HK", "en"] },
    { expected: "zh-TW", languages: ["zh-MO", "en"] },
    // Simplified markers and a bare "zh" map to zh-CN.
    { expected: "zh-CN", languages: ["zh-Hans-CN", "en"] },
    { expected: "zh-CN", languages: ["zh-SG", "en"] },
    { expected: "zh-CN", languages: ["zh", "en"] },
  ])("keeps Chinese locale $expected for $languages", async ({ expected, languages }) => {
    expect(await resolveLanguage(languages)).toBe(expected);
  });

  it("still honors a genuinely preferred non-English language", async () => {
    expect(await resolveLanguage(["hi"])).toBe("hi");
    expect(await resolveLanguage(["hi-IN", "en"])).toBe("hi");
  });
});
