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
import { render, screen } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { StoreValueCell, resolveStoreValue } from "./StoreValueCell";

// Mock the JSON viewer so tests don't have to load Monaco; assert via the serialized content.
vi.mock("src/components/RenderedJsonField", () => ({
  default: ({ content }: { readonly content: object }) => (
    <div data-testid="json-field">{JSON.stringify(content)}</div>
  ),
}));

describe("resolveStoreValue", () => {
  it.each([
    ["a plain object", { count: 2, nested: { ok: true } }, { json: { count: 2, nested: { ok: true } } }],
    ["an array", [1, 2, 3], { json: [1, 2, 3] }],
    ["a JSON object string", '{"job_id": "abc", "n1": 1}', { json: { job_id: "abc", n1: 1 } }],
    ["a JSON array string", "[1, 2, 3]", { json: [1, 2, 3] }],
  ])("renders %s in the JSON viewer", (_label, value, expected) => {
    expect(resolveStoreValue(value)).toStrictEqual(expected);
  });

  it.each([
    ["a non-JSON string", "polling", "polling"],
    ["an empty string", "", ""],
    ["a malformed JSON string", "{oops", "{oops"],
    // Valid JSON, but a primitive — not worth a JSON editor, so it stays text.
    ["a numeric JSON string", "42", "42"],
    ["a boolean JSON string", "true", "true"],
    ['the string "null"', "null", "null"],
    ["a number", 42, "42"],
    ["a boolean", false, "false"],
    ["null", null, "null"],
    ["undefined", undefined, "undefined"],
  ])("renders %s as text", (_label, value, expected) => {
    expect(resolveStoreValue(value)).toStrictEqual({ text: expected });
  });
});

describe("StoreValueCell", () => {
  it("renders objects in the JSON viewer", () => {
    render(<StoreValueCell value={{ key: "value" }} />, { wrapper: Wrapper });

    expect(screen.getByTestId("json-field")).toHaveTextContent('{"key":"value"}');
  });

  it("parses and renders JSON-encoded strings in the JSON viewer", () => {
    render(<StoreValueCell value='{"key": "value"}' />, { wrapper: Wrapper });

    expect(screen.getByTestId("json-field")).toHaveTextContent('{"key":"value"}');
  });

  it("renders plain strings as text", () => {
    render(<StoreValueCell value="just a string" />, { wrapper: Wrapper });

    expect(screen.queryByTestId("json-field")).toBeNull();
    expect(screen.getByText("just a string")).toBeInTheDocument();
  });

  it("renders numbers as text", () => {
    render(<StoreValueCell value={42} />, { wrapper: Wrapper });

    expect(screen.queryByTestId("json-field")).toBeNull();
    expect(screen.getByText("42")).toBeInTheDocument();
  });
});
