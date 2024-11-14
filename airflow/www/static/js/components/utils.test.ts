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

import jsonParse from "./utils";

/* global describe, test, expect */

describe("JSON Parsing.", () => {
  test.each([
    {
      testName: "null",
      testContent: null,
      expectedIsJson: false,
    },
    {
      testName: "boolean",
      testContent: true,
      expectedIsJson: false,
    },
    {
      testName: "int",
      testContent: 42,
      expectedIsJson: false,
    },
    {
      testName: "float",
      testContent: 3.1415,
      expectedIsJson: false,
    },
    {
      testName: "string",
      testContent: "hello world",
      expectedIsJson: false,
    },
    {
      testName: "array",
      testContent: ["hello world", 42, 3.1515],
      expectedIsJson: true,
    },
    {
      testName: "array as string",
      testContent: JSON.stringify(["hello world", 42, 3.1515]),
      expectedIsJson: true,
    },
    {
      testName: "dict",
      testContent: { key: 42 },
      expectedIsJson: true,
    },
    {
      testName: "dict as string",
      testContent: JSON.stringify({ key: 42 }),
      expectedIsJson: true,
    },
  ])(
    "Input value is $testName",
    ({ testName, testContent, expectedIsJson }) => {
      const [isJson, contentJson, contentFormatted] = jsonParse(testContent);

      expect(testName).not.toBeNull();
      expect(isJson).toEqual(expectedIsJson);
      if (expectedIsJson) {
        expect(contentJson).not.toBeNull();
        expect(contentFormatted.length).toBeGreaterThan(0);
      }
    }
  );
});
