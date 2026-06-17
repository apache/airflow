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
import i18n from "i18next";
import LanguageDetector from "i18next-browser-languagedetector";
import Backend from "i18next-http-backend";
import { initReactI18next } from "react-i18next";

import { VersionService } from "openapi/requests/services.gen";

export const supportedLanguages = [
  { code: "en", name: "English" },
  { code: "ar", name: "العربية" },
  { code: "ca", name: "Català" },
  { code: "de", name: "Deutsch" },
  { code: "el", name: "Ελληνικά" },
  { code: "es", name: "Español" },
  { code: "fr", name: "Français" },
  { code: "he", name: "עברית" },
  { code: "hi", name: "हिन्दी" },
  { code: "hu", name: "Magyar" },
  { code: "it", name: "Italiano" },
  { code: "ja", name: "日本語" },
  { code: "ko", name: "한국어" },
  { code: "nl", name: "Nederlands" },
  { code: "pl", name: "Polski" },
  { code: "pt", name: "Português" },
  { code: "ru", name: "Русский" },
  { code: "th", name: "ไทย" },
  { code: "tr", name: "Türkçe" },
  { code: "zh-CN", name: "简体中文" },
  { code: "zh-TW", name: "繁體中文" },
] as const;

export const defaultLanguage = "en";
export const namespaces = [
  "common",
  "dashboard",
  "dags",
  "admin",
  "browse",
  "assets",
  "components",
  "hitl",
] as const;

const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
const baseUrl = new URL(baseHref, globalThis.location.origin);
const basePath = new URL(baseUrl).pathname.replace(/\/$/u, "");

const supportedCodes: Array<string> = supportedLanguages.map((lang) => lang.code);

// i18next resolves navigator.languages with two global passes: it only reduces
// a region code ("en-GB") to its base ("en") when NO exact match exists anywhere
// in the list. So a browser list like ["en-GB", "hi-IN", "hi", "en-US", "en"]
// matched the exact "hi" before "en-GB" was ever reduced to "en", and the UI
// loaded in Hindi. The detector maps this function over each navigator.languages
// entry (preserving order), so normalising per code makes the base-language
// match happen in priority order. Preferred over nonExplicitSupportedLngs /
// load:"languageOnly", which strip the region from every code and so collapse
// "zh-CN"/"zh-TW" to the unsupported "zh".
export const convertDetectedLanguage = (lng: string): string => {
  let locale: Intl.Locale;

  try {
    locale = new Intl.Locale(lng);
  } catch {
    // Not a well-formed BCP-47 tag (e.g. a stale "zh_TW" cache value). Leave it
    // for i18next to fall back rather than guess.
    return lng;
  }

  // Exact supported code, tolerating browser casing ("ZH-CN" -> "zh-CN").
  if (supportedCodes.includes(locale.baseName)) {
    return locale.baseName;
  }

  // Region/script variant of a supported base language: "en-GB" -> "en", "pt-BR" -> "pt".
  if (supportedCodes.includes(locale.language)) {
    return locale.language;
  }

  // For a language whose base code isn't supported but which has region-specific
  // supported locales, match by language + resolved script. maximize() derives
  // the script from the region via CLDR data, so "zh-HK"/"zh-Hant" (Hant) map to
  // "zh-TW" and "zh"/"zh-SG" (Hans) map to "zh-CN". A Chinese tag thus prefers
  // Chinese over a lower-priority language.
  const wanted = locale.maximize();

  return (
    supportedCodes.find((code) => {
      const candidate = new Intl.Locale(code).maximize();

      return candidate.language === wanted.language && candidate.script === wanted.script;
    }) ?? lng
  );
};

export const i18nBaseOptions = {
  defaultNS: "common",
  detection: {
    caches: ["localStorage"],
    convertDetectedLanguage,
    order: ["localStorage", "navigator", "htmlTag"],
  },
  fallbackLng: defaultLanguage,
  interpolation: {
    escapeValue: false,
  },
  ns: namespaces,
  react: {
    useSuspense: false,
  },
  supportedLngs: supportedCodes,
};

const initI18n = (version: string) => {
  const queryString = version ? `?v=${version}` : "";

  void i18n
    .use(Backend)
    .use(LanguageDetector)
    .use(initReactI18next)
    .init({
      ...i18nBaseOptions,
      backend: {
        loadPath: `${basePath}/static/i18n/locales/{{lng}}/{{ns}}.json${queryString}`,
      },
    });
};

void VersionService.getVersion()
  .then((data) => {
    initI18n(data.version);
  })
  .catch(() => {
    initI18n("");
  });

export { default } from "i18next";
