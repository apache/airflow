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
import dayjs from "dayjs";
import "dayjs/locale/ar";
import "dayjs/locale/ca";
import "dayjs/locale/de";
import "dayjs/locale/el";
import "dayjs/locale/es";
import "dayjs/locale/fr";
import "dayjs/locale/he";
import "dayjs/locale/hi";
import "dayjs/locale/hu";
import "dayjs/locale/it";
import "dayjs/locale/ja";
import "dayjs/locale/ko";
import "dayjs/locale/nl";
import "dayjs/locale/pl";
import "dayjs/locale/pt";
import "dayjs/locale/ru";
import "dayjs/locale/th";
import "dayjs/locale/tr";
import "dayjs/locale/zh-cn";
import "dayjs/locale/zh-tw";
import type { i18n as I18nInstance } from "i18next";

// dayjs holds a single global locale and bundles only `en`, so `.humanize()` and
// `.fromNow()` rendered English durations ("2 hours") inside otherwise translated
// sentences. The locale data is registered eagerly rather than fetched per language:
// a dynamic import resolves after react-i18next has already re-rendered on
// `languageChanged`, which would leave the previous language's durations on screen
// until something else triggered a render. The twenty files cost ~7.6 kB gzipped.
//
// Keys are i18next codes from `supportedLanguages`; values are dayjs locale names,
// which are lower-cased and so differ for the Chinese variants. `pt` maps to European
// Portuguese because that is what the bare `pt` code asks for.
const DAYJS_LOCALES: Record<string, string> = {
  ar: "ar",
  ca: "ca",
  de: "de",
  el: "el",
  en: "en",
  es: "es",
  fr: "fr",
  he: "he",
  hi: "hi",
  hu: "hu",
  it: "it",
  ja: "ja",
  ko: "ko",
  nl: "nl",
  pl: "pl",
  pt: "pt",
  ru: "ru",
  th: "th",
  tr: "tr",
  "zh-CN": "zh-cn",
  "zh-TW": "zh-tw",
};

export const dayjsLocaleCodes = Object.keys(DAYJS_LOCALES);

const FALLBACK_DAYJS_LOCALE = "en";

export const syncDayjsLocale = (language: string): void => {
  dayjs.locale(DAYJS_LOCALES[language] ?? FALLBACK_DAYJS_LOCALE);
};

// Register this on i18next before `init()`: the emitter runs `languageChanged`
// callbacks in subscription order and react-i18next subscribes each component as it
// mounts, so subscribing first is what guarantees dayjs has switched by the time any
// component re-renders in the new language.
export const registerDayjsLocaleSync = (instance: I18nInstance): void => {
  instance.on("languageChanged", syncDayjsLocale);
};
