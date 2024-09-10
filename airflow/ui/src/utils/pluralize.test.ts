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

import { pluralize } from "./pluralize";

type PluralizeTestCase = {
  in: [string, number, (string | null)?, boolean?];
  out: string;
};

const pluralizeTestCases: PluralizeTestCase[] = [
  { in: ["DAG", 0, undefined, undefined], out: "0 DAGs" },
  { in: ["DAG", 1, undefined, undefined], out: "1 DAG" },
  { in: ["DAG", 12000, undefined, undefined], out: "12,000 DAGs" },
  { in: ["DAG", 12000000, undefined, undefined], out: "12,000,000 DAGs" },
  { in: ["DAG", 0, undefined, undefined], out: "0 DAGs" },
  { in: ["DAG", 1, undefined, undefined], out: "1 DAG" },
  { in: ["DAG", 12000, undefined, undefined], out: "12,000 DAGs" },
  { in: ["DAG", 12000000, undefined, undefined], out: "12,000,000 DAGs" },
  // Omit the count.
  { in: ["DAG", 0, null, true], out: "DAGs" },
  { in: ["DAG", 1, null, true], out: "DAG" },
  { in: ["DAG", 12000, null, true], out: "DAGs" },
  { in: ["DAG", 12000000, null, true], out: "DAGs" },
  { in: ["DAG", 0, null, true], out: "DAGs" },
  { in: ["DAG", 1, null, true], out: "DAG" },
  { in: ["DAG", 12000, null, true], out: "DAGs" },
  { in: ["DAG", 12000000, null, true], out: "DAGs" },
  // The casing of the string is preserved.
  { in: ["goose", 0, "geese", undefined], out: "0 geese" },
  { in: ["goose", 1, "geese", undefined], out: "1 goose" },
  // The plural form is different from the singular form.
  { in: ["Goose", 0, "Geese", undefined], out: "0 Geese" },
  { in: ["Goose", 1, "Geese", undefined], out: "1 Goose" },
  { in: ["Goose", 12000, "Geese", undefined], out: "12,000 Geese" },
  { in: ["Goose", 12000000, "Geese", undefined], out: "12,000,000 Geese" },
  { in: ["Goose", 0, "Geese", undefined], out: "0 Geese" },
  { in: ["Goose", 1, "Geese", undefined], out: "1 Goose" },
  { in: ["Goose", 12000, "Geese", undefined], out: "12,000 Geese" },
  { in: ["Goose", 12000000, "Geese", undefined], out: "12,000,000 Geese" },
  // In the case of "Moose", the plural is the same as the singular and you
  // probably wouldn't elect to use this function at all, but there could be
  // cases where dynamic data makes it unavoidable.
  { in: ["Moose", 0, "Moose", undefined], out: "0 Moose" },
  { in: ["Moose", 1, "Moose", undefined], out: "1 Moose" },
  { in: ["Moose", 12000, "Moose", undefined], out: "12,000 Moose" },
  { in: ["Moose", 12000000, "Moose", undefined], out: "12,000,000 Moose" },
  { in: ["Moose", 0, "Moose", undefined], out: "0 Moose" },
  { in: ["Moose", 1, "Moose", undefined], out: "1 Moose" },
  { in: ["Moose", 12000, "Moose", undefined], out: "12,000 Moose" },
  { in: ["Moose", 12000000, "Moose", undefined], out: "12,000,000 Moose" },
];

describe("pluralize", () => {
  it("case", () => {
    pluralizeTestCases.forEach((testCase) =>
      expect(
        pluralize(
          testCase.in[0],
          testCase.in[1],
          testCase.in[2],
          testCase.in[3]
        )
      ).toEqual(testCase.out)
    );
  });
});
