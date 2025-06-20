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

export const supportedLanguages = [
  { code: "ar", flag: "🇸🇦", name: "العربية" },
  { code: "de", flag: "🇩🇪", name: "Deutsch" },
  { code: "en", flag: "🇺🇸", name: "English" },
  { code: "he", flag: "🇮🇱", name: "עברית" },
  { code: "ko", flag: "🇰🇷", name: "한국어" },
  { code: "nl", flag: "🇳🇱", name: "Nederlands" },
  { code: "pl", flag: "🇵🇱", name: "Polski" },
  { code: "zh-TW", flag: "🇹🇼", name: "繁體中文" },
  { code: "fr", flag: "🇫🇷", name: "Français" },
] as const;

export const defaultLanguage = "en";
export const namespaces = ["common", "dashboard", "dags", "admin", "browse", "assets", "components"] as const;

void i18n
  .use(Backend)
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    backend: {
      loadPath: "/static/i18n/locales/{{lng}}/{{ns}}.json",
    },
    defaultNS: "common",
    detection: {
      caches: ["localStorage"],
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
    supportedLngs: supportedLanguages.map((lang) => lang.code),
  });

export { default } from "i18next";
