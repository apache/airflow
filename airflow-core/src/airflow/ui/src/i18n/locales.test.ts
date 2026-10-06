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
import { createInstance, type Resource } from "i18next";
import { describe, expect, it } from "vitest";

import { i18nBaseOptions } from "./config";

const localeFiles: Record<string, Record<string, unknown>> = import.meta.glob(
  "../../public/i18n/locales/*/*.json",
  { eager: true, import: "default" },
);

const resources: Resource = {};

for (const [path, content] of Object.entries(localeFiles)) {
  const { lng = "", ns = "" } = /locales\/(?<lng>[^/]+)\/(?<ns>[^/]+)\.json$/u.exec(path)?.groups ?? {};

  resources[lng] = { ...resources[lng], [ns]: content };
}

const flattenStrings = (node: unknown, prefix: string): Array<[string, string]> => {
  if (typeof node === "string") {
    return [[prefix, node]];
  }

  return typeof node === "object" && node !== null
    ? Object.entries(node).flatMap(([key, value]) => flattenStrings(value, `${prefix}.${key}`))
    : [];
};

// Counter placeholders must go through i18next's built-in `number` formatter so that
// they get the locale's digit grouping (1,234 / 1.234 / 1 234).
const UNFORMATTED_COUNTER = /\{\{\s*(?:count|current|missedCount|total|upcomingCount)\s*\}\}/u;

const localeStrings = Object.entries(resources).flatMap(([lng, namespaces]) =>
  flattenStrings(namespaces, lng),
);

describe("locale counter placeholders", () => {
  it("formats every counter placeholder as a number", () => {
    const unformatted = localeStrings
      .filter(([, value]) => UNFORMATTED_COUNTER.test(value))
      .map(([key]) => key);

    expect(unformatted).toEqual([]);
  });

  it.each([
    { expected: "1,234 Tasks", lng: "en" },
    { expected: "1.234 Tasks", lng: "de" },
  ])("groups thousands of a plural count in $lng", async ({ expected, lng }) => {
    const instance = createInstance();

    await instance.init({ ...i18nBaseOptions, lng, resources });

    expect(instance.t("components:graph.taskCount", { count: 1234 })).toBe(expected);
  });

  it.each([
    { expected: "1,234 of 56,789", lng: "en" },
    { expected: "1.234 von 56.789", lng: "de" },
  ])("groups thousands of non-plural counters in $lng", async ({ expected, lng }) => {
    const instance = createInstance();

    await instance.init({ ...i18nBaseOptions, lng, resources });

    expect(instance.t("dag:logs.search.matchCount", { current: 1234, total: 56_789 })).toBe(expected);
  });

  it("renders a formatted counter passed as undefined as blank, like an unformatted one", async () => {
    const instance = createInstance();

    await instance.init({ ...i18nBaseOptions, lng: "en", resources });

    expect(instance.t("dag:logs.search.matchCount", { current: undefined, total: 5 })).toBe(" of 5");
  });

  it("keeps the placeholder of a counter that is not passed at all", async () => {
    const instance = createInstance();

    await instance.init({ ...i18nBaseOptions, lng: "en", resources });

    expect(instance.t("dag:logs.search.matchCount", { total: 5 })).toBe("{{current, number}} of 5");
  });
});
