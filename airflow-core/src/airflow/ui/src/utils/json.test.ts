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

import { isJsonString, minifyJson, prettifyJson } from "./json";

describe("JSON utilities", () => {
  describe("isJsonString", () => {
    it("should return true for valid JSON object", () => {
      expect(isJsonString('{"key": "value"}')).toBe(true);
    });

    it("should return true for valid JSON array", () => {
      expect(isJsonString('["value1", "value2"]')).toBe(true);
    });

    it("should return true for valid JSON primitives", () => {
      expect(isJsonString('"string"')).toBe(true);
      expect(isJsonString("123")).toBe(true);
      expect(isJsonString("true")).toBe(true);
      expect(isJsonString("null")).toBe(true);
    });

    it("should return false for invalid JSON", () => {
      expect(isJsonString("{invalid}")).toBe(false);
      expect(isJsonString("")).toBe(false);
      expect(isJsonString("not json")).toBe(false);
    });
  });

  describe("prettifyJson", () => {
    it("should prettify valid JSON object", () => {
      const input = '{"key":"value","nested":{"a":1}}';
      const expected = '{\n  "key": "value",\n  "nested": {\n    "a": 1\n  }\n}';

      expect(prettifyJson(input)).toBe(expected);
    });

    it("should prettify valid JSON array", () => {
      const input = '["value1","value2"]';
      const expected = '[\n  "value1",\n  "value2"\n]';

      expect(prettifyJson(input)).toBe(expected);
    });

    it("should use custom indentation", () => {
      const input = '{"key":"value"}';
      const expected = '{\n    "key": "value"\n}';

      expect(prettifyJson(input, 4)).toBe(expected);
    });

    it("should return original string for invalid JSON", () => {
      const input = "{invalid}";

      expect(prettifyJson(input)).toBe(input);
    });

    it("should return original string for non-JSON text", () => {
      const input = "plain text";

      expect(prettifyJson(input)).toBe(input);
    });
  });

  describe("minifyJson", () => {
    it("should minify prettified JSON object", () => {
      const input = '{\n  "key": "value",\n  "nested": {\n    "a": 1\n  }\n}';
      const expected = '{"key":"value","nested":{"a":1}}';

      expect(minifyJson(input)).toBe(expected);
    });

    it("should minify prettified JSON array", () => {
      const input = '[\n  "value1",\n  "value2"\n]';
      const expected = '["value1","value2"]';

      expect(minifyJson(input)).toBe(expected);
    });

    it("should handle already minified JSON", () => {
      const input = '{"key":"value"}';

      expect(minifyJson(input)).toBe(input);
    });

    it("should return original string for invalid JSON", () => {
      const input = "{invalid}";

      expect(minifyJson(input)).toBe(input);
    });

    it("should return original string for non-JSON text", () => {
      const input = "plain text";

      expect(minifyJson(input)).toBe(input);
    });
  });
});
