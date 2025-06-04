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

import deCommon from "./locales/de/common.json";
import deComponents from "./locales/de/components.json";
import deConnections from "./locales/de/connections.json";
import deDags from "./locales/de/dags.json";
import deDashboard from "./locales/de/dashboard.json";
import enCommon from "./locales/en/common.json";
import enComponents from "./locales/en/components.json";
import enConnections from "./locales/en/connections.json";
import enDags from "./locales/en/dags.json";
import enDashboard from "./locales/en/dashboard.json";
import koCommon from "./locales/ko/common.json";
import koDashboard from "./locales/ko/dashboard.json";
import nlCommon from "./locales/nl/common.json";
import nlDashboard from "./locales/nl/dashboard.json";
import plCommon from "./locales/pl/common.json";
import plConnections from "./locales/pl/connections.json";
import plDags from "./locales/pl/dags.json";
import plDashboard from "./locales/pl/dashboard.json";
import zhTWCommon from "./locales/zh-TW/common.json";
import zhTWDashboard from "./locales/zh-TW/dashboard.json";

// TODO: Dynamically load translation files
// import Backend from 'i18next-http-backend';

export const supportedLanguages = [
  { code: "de", name: "Deutsch" },
  { code: "en", name: "English" },
  { code: "ko", name: "한국어" },
  { code: "nl", name: "Nederlands" },
  { code: "pl", name: "Polski" },
  { code: "zh-TW", name: "繁體中文" },
] as const;

export const defaultLanguage = "en";
export const namespaces = ["common", "dashboard", "dags", "connections"] as const;

const resources = {
  de: {
    common: deCommon,
    components: deComponents,
    connections: deConnections,
    dags: deDags,
    dashboard: deDashboard,
  },
  en: {
    common: enCommon,
    components: enComponents,
    connections: enConnections,
    dags: enDags,
    dashboard: enDashboard,
  },
  ko: {
    common: koCommon,
    dashboard: koDashboard,
  },
  nl: {
    common: nlCommon,
    dashboard: nlDashboard,
  },
  pl: {
    common: plCommon,
    connections: plConnections,
    dags: plDags,
    dashboard: plDashboard,
  },
  "zh-TW": {
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
