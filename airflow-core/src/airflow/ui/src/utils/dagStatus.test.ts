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
import type { TFunction } from "i18next";
import { beforeAll, describe, expect, it } from "vitest";

import type { DAGWarningResponse, ImportErrorResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";

import componentsLocale from "../../public/i18n/locales/en/components.json";
import dagLocale from "../../public/i18n/locales/en/dag.json";
import dashboardLocale from "../../public/i18n/locales/en/dashboard.json";
import { summarizeDagStatus } from "./dagStatus";

const warning = (message: string): DAGWarningResponse =>
  ({ message, warning_type: "non-existent pool" }) as DAGWarningResponse;

const importError = { filename: "broken.py" } as ImportErrorResponse;

describe("summarizeDagStatus", () => {
  let translate: TFunction;

  beforeAll(() => {
    i18n.addResourceBundle("en", "components", componentsLocale, true, true);
    i18n.addResourceBundle("en", "dag", dagLocale, true, true);
    i18n.addResourceBundle("en", "dashboard", dashboardLocale, true, true);
    translate = i18n.t.bind(i18n);
  });

  it("fills the count into the combined heading", () => {
    const { heading } = summarizeDagStatus(translate, {
      error: new Error("boom"),
      warnings: [warning("one"), warning("two")],
    });

    // The interpolation key is `warningText`; passing the wrong name left the placeholder raw.
    expect(heading).toBe("1 Error and 2 Warnings");
    expect(heading).not.toContain("{{");
  });

  it("lets a parse error name and colour the whole set", () => {
    const summary = summarizeDagStatus(translate, { importError, warnings: [warning("one")] });

    expect(summary.chipLabel).toBe("Dag Import Error");
    expect(summary.severity).toBe("error");
    expect(summary.heading).toBe("1 Error and 1 Warning");
  });

  it("counts a deactivation among the warnings but lets it speak for itself on the chip", () => {
    const summary = summarizeDagStatus(translate, {
      deactivatedMessage: "Deactivated",
      warnings: [warning("one")],
    });

    expect(summary.chipLabel).toBe("Deactivated");
    expect(summary.severity).toBe("warning");
    expect(summary.warningCount).toBe(2);
    expect(summary.heading).toBe("2 Warnings");
  });

  it("names the warnings alone when nothing else is wrong", () => {
    const summary = summarizeDagStatus(translate, { warnings: [warning("one")] });

    expect(summary.chipLabel).toBe("1 Warning");
    expect(summary.heading).toBe("1 Warning");
    expect(summary.severity).toBe("warning");
  });
});
