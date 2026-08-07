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
import "@testing-library/jest-dom";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { JsonEditor } from "./JsonEditor";

vi.mock("src/components/MonacoEditor", () => ({
  default: ({
    onChange,
    value,
  }: {
    readonly onChange?: (value: string | undefined) => void;
    readonly value?: string;
  }) => (
    <textarea aria-label="JSON editor" onChange={(event) => onChange?.(event.target.value)} value={value} />
  ),
}));

vi.mock("src/context/colorMode", () => ({
  useMonacoTheme: () => ({ beforeMount: vi.fn(), theme: "airflow-light" }),
}));

describe("JsonEditor", () => {
  it("passes the raw value through when prettify is off", () => {
    const onChange = vi.fn();

    render(<JsonEditor onChange={onChange} value="{}" />);

    fireEvent.change(screen.getByLabelText("JSON editor"), { target: { value: '{"key":1}' } });

    expect(onChange).toHaveBeenCalledWith('{"key":1}');
  });

  it("prettifies valid JSON and clears the error when prettify is on", () => {
    const onChange = vi.fn();
    const onError = vi.fn();

    render(<JsonEditor onChange={onChange} onError={onError} prettify value="{}" />);

    fireEvent.change(screen.getByLabelText("JSON editor"), { target: { value: '{"key":1}' } });

    expect(onError).toHaveBeenCalledWith(undefined);
    expect(onChange).toHaveBeenCalledWith(JSON.stringify({ key: 1 }, undefined, 2));
  });

  it("does not call onChange when the prettified JSON is unchanged", () => {
    const onChange = vi.fn();
    const formatted = JSON.stringify({ key: 1 }, undefined, 2);

    render(<JsonEditor onChange={onChange} prettify value={formatted} />);

    fireEvent.change(screen.getByLabelText("JSON editor"), { target: { value: '{"key": 1}' } });

    expect(onChange).not.toHaveBeenCalled();
  });

  it("reports a parse error and skips onChange for invalid JSON when prettify is on", () => {
    const onChange = vi.fn();
    const onError = vi.fn();

    render(<JsonEditor onChange={onChange} onError={onError} prettify value="{}" />);

    fireEvent.change(screen.getByLabelText("JSON editor"), { target: { value: "{invalid" } });

    expect(onError).toHaveBeenCalledWith(expect.any(String));
    expect(onChange).not.toHaveBeenCalled();
  });
});
