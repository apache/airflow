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
import deDag from "./locales/de/dag.json";
import deDags from "./locales/de/dags.json";
import deDashboard from "./locales/de/dashboard.json";
import enCommon from "./locales/en/common.json";
import enComponents from "./locales/en/components.json";
import enConnections from "./locales/en/connections.json";
import enDag from "./locales/en/dag.json";
import enDags from "./locales/en/dags.json";
import enDashboard from "./locales/en/dashboard.json";
import heCommon from "./locales/he/common.json";
import heDashboard from "./locales/he/dashboard.json";
import koCommon from "./locales/ko/common.json";
import koDashboard from "./locales/ko/dashboard.json";
import nlCommon from "./locales/nl/common.json";
import nlDashboard from "./locales/nl/dashboard.json";
import plCommon from "./locales/pl/common.json";
import plComponents from "./locales/pl/components.json";
import plConnections from "./locales/pl/connections.json";
import plDag from "./locales/pl/dag.json";
import plDags from "./locales/pl/dags.json";
import plDashboard from "./locales/pl/dashboard.json";
import zhTWCommon from "./locales/zh-TW/common.json";
import zhTWConnections from "./locales/zh-TW/connections.json";
import zhTWDags from "./locales/zh-TW/dags.json";
import zhTWDashboard from "./locales/zh-TW/dashboard.json";

// TODO: Dynamically load translation files
// import Backend from 'i18next-http-backend';

export const supportedLanguages = [
  { code: "de", flag: "🇩🇪", name: "Deutsch" },
  { code: "en", flag: "🇺🇸", name: "English" },
  { code: "he", flag: "🇮🇱", name: "עברית" },
  { code: "ko", flag: "🇰🇷", name: "한국어" },
  { code: "nl", flag: "🇳🇱", name: "Nederlands" },
  { code: "pl", flag: "🇵🇱", name: "Polski" },
  { code: "zh-TW", flag: "🇹🇼", name: "繁體中文" },
] as const;

export const defaultLanguage = "en";
export const namespaces = ["common", "dashboard", "dags", "connections"] as const;

const resources = {
  de: {
    common: deCommon,
    components: deComponents,
    connections: deConnections,
    dag: deDag,
    dags: deDags,
    dashboard: deDashboard,
  },
  en: {
    common: enCommon,
    components: enComponents,
    connections: enConnections,
    dag: enDag,
    dags: enDags,
    dashboard: enDashboard,
  },
  he: {
    common: heCommon,
    dashboard: heDashboard,
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
    components: plComponents,
    connections: plConnections,
    dag: plDag,
    dags: plDags,
    dashboard: plDashboard,
  },
  "zh-TW": {
    common: zhTWCommon,
    connections: zhTWConnections,
    dags: zhTWDags,
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
