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

import { describe, expect, it } from "vitest";

import { warnOnSuspiciousIds } from "../../src/cli/validate.js";
import type { BundleManifest } from "../../src/coordinator/manifest.js";

// U+20000, a CJK letter outside the Basic Multilingual Plane: one code point
// but two UTF-16 units, so it separates code-point from .length counting.
const ASTRAL_LETTER = "𠀀";

function warningsFor(dags: BundleManifest["dags"]): string[] {
  const warnings: string[] = [];
  warnOnSuspiciousIds(dags, (message) => warnings.push(message));
  return warnings;
}

function dagCharsetWarning(id: string): string {
  return `warning: dag id ${JSON.stringify(id)} must be made of alphanumeric characters, dashes, dots, and underscores; the Airflow server will reject it`;
}

describe("warnOnSuspiciousIds", () => {
  it.each([
    "simple",
    "with-dash",
    "with.dot",
    "with_underscore",
    "0numeric",
    "café_dag",
    "任務",
    "a".repeat(250),
    "任".repeat(250),
    ASTRAL_LETTER.repeat(250),
  ])("does not warn on valid id %j", (id) => {
    expect(warningsFor({ [id]: { tasks: [id] } })).toEqual([]);
  });

  it.each(["a".repeat(251), "任".repeat(251), ASTRAL_LETTER.repeat(251)])(
    "warns on an id longer than 250 code points: %j",
    (id) => {
      expect(warningsFor({ [id]: { tasks: [] } })).toEqual([
        `warning: dag id ${JSON.stringify(id)} is longer than 250 characters (251); the Airflow server will reject it`,
      ]);
    },
  );

  // "a..b c" also locks the else-if: a charset failure suppresses the '..' warning.
  it.each(["", "with space", "with/slash", "with:colon", "with\ttab", "a..b c"])(
    "warns on an id with invalid characters: %j",
    (id) => {
      expect(warningsFor({ [id]: { tasks: [] } })).toEqual([dagCharsetWarning(id)]);
    },
  );

  it("warns on an id containing double dots", () => {
    expect(warningsFor({ "a..b": { tasks: [] } })).toEqual([
      `warning: dag id "a..b" contains '..'; the Airflow server will reject it unless [core] allow_double_dot_in_ids is enabled`,
    ]);
  });

  it("warns twice on an id that is both too long and invalid", () => {
    const id = "a".repeat(250) + " b";
    expect(warningsFor({ [id]: { tasks: [] } })).toEqual([
      `warning: dag id ${JSON.stringify(id)} is longer than 250 characters (252); the Airflow server will reject it`,
      dagCharsetWarning(id),
    ]);
  });

  it("sorts dag ids for stable output", () => {
    const warnings = warningsFor({
      "delta d": { tasks: [] },
      "alpha d": { tasks: [] },
      "charlie d": { tasks: [] },
      "bravo d": { tasks: [] },
    });
    expect(warnings).toEqual(["alpha d", "bravo d", "charlie d", "delta d"].map(dagCharsetWarning));
  });

  it("names the owning dag in a task id warning", () => {
    expect(warningsFor({ my_dag: { tasks: ["bad task"] } })).toEqual([
      `warning: task id "bad task" in dag "my_dag" must be made of alphanumeric characters, dashes, dots, and underscores; the Airflow server will reject it`,
    ]);
  });
});
