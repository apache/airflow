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

import arAdmin from "./locales/ar/admin.json";
import arAssets from "./locales/ar/assets.json";
import arBrowse from "./locales/ar/browse.json";
import arCommon from "./locales/ar/common.json";
import arComponents from "./locales/ar/components.json";
import arDag from "./locales/ar/dag.json";
import arDags from "./locales/ar/dags.json";
import arDashboard from "./locales/ar/dashboard.json";
import deAdmin from "./locales/de/admin.json";
import deAssets from "./locales/de/assets.json";
import deBrowse from "./locales/de/browse.json";
import deCommon from "./locales/de/common.json";
import deComponents from "./locales/de/components.json";
import deDag from "./locales/de/dag.json";
import deDags from "./locales/de/dags.json";
import deDashboard from "./locales/de/dashboard.json";
import enAdmin from "./locales/en/admin.json";
import enAssets from "./locales/en/assets.json";
import enBrowse from "./locales/en/browse.json";
import enCommon from "./locales/en/common.json";
import enComponents from "./locales/en/components.json";
import enDag from "./locales/en/dag.json";
import enDags from "./locales/en/dags.json";
import enDashboard from "./locales/en/dashboard.json";
import heAdmin from "./locales/he/admin.json";
import heAsset from "./locales/he/assets.json";
import heBrowse from "./locales/he/browse.json";
import heCommon from "./locales/he/common.json";
import heComponents from "./locales/he/components.json";
import heDag from "./locales/he/dag.json";
import heDags from "./locales/he/dags.json";
import heDashboard from "./locales/he/dashboard.json";
import koCommon from "./locales/ko/common.json";
import koDashboard from "./locales/ko/dashboard.json";
import nlCommon from "./locales/nl/common.json";
import nlDashboard from "./locales/nl/dashboard.json";
import plAdmin from "./locales/pl/admin.json";
import plAssets from "./locales/pl/assets.json";
import plBrowse from "./locales/pl/browse.json";
import plCommon from "./locales/pl/common.json";
import plComponents from "./locales/pl/components.json";
import plDag from "./locales/pl/dag.json";
import plDags from "./locales/pl/dags.json";
import plDashboard from "./locales/pl/dashboard.json";
import zhTWAdmin from "./locales/zh-TW/admin.json";
import zhTWAssets from "./locales/zh-TW/assets.json";
import zhTWBrowse from "./locales/zh-TW/browse.json";
import zhTWCommon from "./locales/zh-TW/common.json";
import zhTWComponents from "./locales/zh-TW/components.json";
import zhTWDag from "./locales/zh-TW/dag.json";
import zhTWDags from "./locales/zh-TW/dags.json";
import zhTWDashboard from "./locales/zh-TW/dashboard.json";

// TODO: Dynamically load translation files
// import Backend from 'i18next-http-backend';

export const supportedLanguages = [
  { code: "ar", flag: "🇸🇦", name: "العربية" },
  { code: "de", flag: "🇩🇪", name: "Deutsch" },
  { code: "en", flag: "🇺🇸", name: "English" },
  { code: "he", flag: "🇮🇱", name: "עברית" },
  { code: "ko", flag: "🇰🇷", name: "한국어" },
  { code: "nl", flag: "🇳🇱", name: "Nederlands" },
  { code: "pl", flag: "🇵🇱", name: "Polski" },
  { code: "zh-TW", flag: "🇹🇼", name: "繁體中文" },
] as const;

export const defaultLanguage = "en";
export const namespaces = ["common", "dashboard", "dags", "admin", "browse", "assets"] as const;

const resources = {
  ar: {
    admin: arAdmin,
    assets: arAssets,
    browse: arBrowse,
    common: arCommon,
    components: arComponents,
    dag: arDag,
    dags: arDags,
    dashboard: arDashboard,
  },
  de: {
    admin: deAdmin,
    assets: deAssets,
    browse: deBrowse,
    common: deCommon,
    components: deComponents,
    dag: deDag,
    dags: deDags,
    dashboard: deDashboard,
  },
  en: {
    admin: enAdmin,
    assets: enAssets,
    browse: enBrowse,
    common: enCommon,
    components: enComponents,
    dag: enDag,
    dags: enDags,
    dashboard: enDashboard,
  },
  he: {
    admin: heAdmin,
    assets: heAsset,
    browse: heBrowse,
    common: heCommon,
    components: heComponents,
    dag: heDag,
    dags: heDags,
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
    admin: plAdmin,
    assets: plAssets,
    browse: plBrowse,
    common: plCommon,
    components: plComponents,
    dag: plDag,
    dags: plDags,
    dashboard: plDashboard,
  },
  "zh-TW": {
    admin: zhTWAdmin,
    assets: zhTWAssets,
    browse: zhTWBrowse,
    common: zhTWCommon,
    components: zhTWComponents,
    dag: zhTWDag,
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
