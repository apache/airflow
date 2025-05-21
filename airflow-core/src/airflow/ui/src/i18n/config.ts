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
import { initReactI18next } from "react-i18next";

import enCommon from "./locales/en/common.json";
import enDashboard from "./locales/en/dashboard.json";
import zhTWCommon from "./locales/zh_TW/common.json";
import zhTWDashboard from "./locales/zh_TW/dashboard.json";

// TODO: Dynamically load translation files
// import Backend from 'i18next-http-backend';

export const supportedLanguages = [
  { code: "en", name: "English" },
  { code: "zh_TW", name: "繁體中文" },
] as const;

export const defaultLanguage = "en";
export const namespaces = ["common", "dashboard"] as const;

const resources = {
  en: {
    common: enCommon,
    dashboard: enDashboard,
  },
  zh_TW: {
    common: zhTWCommon,
    dashboard: zhTWDashboard,
  },
};

void i18n
  // .use(Backend) // TODO: Dynamically load translation files
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
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
    resources,
    supportedLngs: supportedLanguages.map((lang) => lang.code),
  });

export { default } from "i18next";
