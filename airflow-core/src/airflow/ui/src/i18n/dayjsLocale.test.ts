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
import dayjsDuration from "dayjs/plugin/duration";
import relativeTime from "dayjs/plugin/relativeTime";
import { createInstance } from "i18next";
import { afterEach, describe, expect, it } from "vitest";

import { i18nBaseOptions, supportedLanguages } from "./config";
import { dayjsLocaleCodes, registerDayjsLocaleSync, syncDayjsLocale } from "./dayjsLocale";

dayjs.extend(dayjsDuration);
dayjs.extend(relativeTime);

const humanizeTwoHours = () => dayjs.duration(2, "hours").humanize();

// dayjs keeps the locale in module-global state, so a test that switched it would
// otherwise decide what every later test formats.
afterEach(() => {
  dayjs.locale("en");
});

describe("dayjs locale", () => {
  it("covers exactly the languages the UI offers", () => {
    expect([...dayjsLocaleCodes].sort()).toStrictEqual(
      supportedLanguages.map((language) => language.code).sort(),
    );
  });

  it.each(supportedLanguages.filter((language) => language.code !== "en"))(
    "humanizes a duration in $code rather than English",
    ({ code }) => {
      syncDayjsLocale(code);

      expect(humanizeTwoHours()).not.toBe("2 hours");
    },
  );

  it("renders the reported Arabic case", () => {
    syncDayjsLocale("ar");

    expect(humanizeTwoHours()).toBe("2 ساعات");
  });

  it("falls back to English for a language dayjs does not ship", () => {
    syncDayjsLocale("ar");
    syncDayjsLocale("cy");

    expect(humanizeTwoHours()).toBe("2 hours");
  });

  it("applies the language i18next resolves during init", async () => {
    const instance = createInstance();

    registerDayjsLocaleSync(instance);
    await instance.init({ ...i18nBaseOptions, lng: "ar", resources: {} });

    expect(humanizeTwoHours()).toBe("2 ساعات");
  });

  it("follows a later language switch", async () => {
    const instance = createInstance();

    registerDayjsLocaleSync(instance);
    await instance.init({ ...i18nBaseOptions, lng: "ar", resources: {} });
    await instance.changeLanguage("zh-CN");

    expect(humanizeTwoHours()).toBe("2 小时");
  });
});
