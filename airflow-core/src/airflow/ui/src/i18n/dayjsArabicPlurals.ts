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
import arLocale from "dayjs/locale/ar";
import updateLocale from "dayjs/plugin/updateLocale";

dayjs.extend(updateLocale);

// Arabic inflects a counted noun four ways, but dayjs's `ar` locale stores a single
// template per unit ("%d ساعات") and so renders the 3-10 form for every count above
// one: "2 ساعات" where the dual "ساعتين" is required, "11 ساعات" where the accusative
// "11 ساعة" is. Hebrew, Russian and Polish have comparable plural systems and already
// pass a function instead of a template; Arabic was never migrated. Delete this module
// once dayjs ships the same treatment and the dependency is bumped.
//
// Forms are [two, few (n%100 = 3-10), many (n%100 = 11-99), other]. "many" takes the
// accusative, written with tanwīn on masculine nouns (يومًا) and identical to the
// nominative on feminine ones (ساعة). Counts 3 and above are verified against
// Intl.DurationFormat, which reads the same CLDR data these templates were
// transcribed from.
//
// The dual deliberately departs from Intl, which formats a standalone duration and so
// yields the nominative (ساعتان). Every humanized duration in this UI is governed by a
// preposition -- "خلال {{interval}}" in the deadline rule, and dayjs's own past/future
// wrappers ("منذ %s", "بعد %s") around every `fromNow()` -- which requires the oblique
// (خلال ساعتين). The dual is the only form that spells its case out; on the others the
// ending is an unwritten vowel, and the 11-99 form is تمييز, invariably accusative
// whatever governs it. Revisit if a caller ever renders a duration standing alone.
const ARABIC_UNIT_FORMS = {
  dd: ["يومين", "أيام", "يومًا", "يوم"],
  hh: ["ساعتين", "ساعات", "ساعة", "ساعة"],
  mm: ["دقيقتين", "دقائق", "دقيقة", "دقيقة"],
  MM: ["شهرين", "أشهر", "شهرًا", "شهر"],
  yy: ["عامين", "أعوام", "عامًا", "عام"],
} as const;

const formatArabicUnit = (count: number, forms: readonly [string, string, string, string]): string => {
  const [two, few, many, other] = forms;

  if (count === 2) {
    return two;
  }

  const remainder = count % 100;

  if (remainder >= 3 && remainder <= 10) {
    return `${count} ${few}`;
  }

  return remainder >= 11 && remainder <= 99 ? `${count} ${many}` : `${count} ${other}`;
};

// updateLocale merges with Object.assign, so relativeTime is spread whole — passing
// only the overridden keys would drop future/past and every singular form.
export const applyArabicPluralForms = (): void => {
  dayjs.updateLocale("ar", {
    relativeTime: {
      ...arLocale.relativeTime,
      dd: (count: number) => formatArabicUnit(count, ARABIC_UNIT_FORMS.dd),
      hh: (count: number) => formatArabicUnit(count, ARABIC_UNIT_FORMS.hh),
      mm: (count: number) => formatArabicUnit(count, ARABIC_UNIT_FORMS.mm),
      MM: (count: number) => formatArabicUnit(count, ARABIC_UNIT_FORMS.MM),
      yy: (count: number) => formatArabicUnit(count, ARABIC_UNIT_FORMS.yy),
    },
  });
};
