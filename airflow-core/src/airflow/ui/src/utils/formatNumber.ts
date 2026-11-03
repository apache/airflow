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
import { createIntlCache } from "./intlCache";

const numberFormatter = createIntlCache<Intl.NumberFormat>();

/**
 * Locale digit grouping for counters rendered outside translations; use instead of `toLocaleString`.
 *
 * Components pass `i18n.language` from `useTranslation`, which the React Compiler tracks (react-i18next
 * hands out a new `i18n` wrapper on every language switch). A language read from the i18next singleton
 * in here is invisible to the compiler, so a memoized counter would keep its old grouping.
 */
export const formatNumber = (value: number, locale: string): string =>
  numberFormatter("number", locale, (forLocale) => new Intl.NumberFormat(forLocale)).format(value);
