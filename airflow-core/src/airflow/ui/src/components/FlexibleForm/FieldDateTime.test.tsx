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
import { fireEvent, render } from "@testing-library/react";
import { describe, it, expect, beforeEach, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FieldDateTime } from "./FieldDateTime";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mockParamsDict: Record<string, any> = {};
const mockSetParamsDict = vi.fn();

vi.mock("src/queries/useParamStore", () => ({
  paramPlaceholder: {
    schema: {},
    value: null,
  },
  useParamStore: () => ({
    disabled: false,
    paramsDict: mockParamsDict,
    setParamsDict: mockSetParamsDict,
  }),
}));

const getInputByName = (name: string) =>
  document.querySelector<HTMLInputElement>(`#element_${name}`) as HTMLInputElement;

describe("FieldDateTime — time field (issue #66492)", () => {
  beforeEach(() => {
    mockSetParamsDict.mockClear();
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
  });

  it("renders time input with step=1 so the picker exposes the seconds field", () => {
    mockParamsDict.cutoff_time = {
      schema: { format: "time", type: "string" },
      value: null,
    };

    render(<FieldDateTime name="cutoff_time" onUpdate={vi.fn()} type="time" />, {
      wrapper: Wrapper,
    });

    const input = getInputByName("cutoff_time");

    expect(input.type).toBe("time");
    expect(input.getAttribute("step")).toBe("1");
  });

  it("does not set step on a date input", () => {
    mockParamsDict.cutoff_date = {
      schema: { format: "date", type: "string" },
      value: null,
    };

    render(<FieldDateTime name="cutoff_date" onUpdate={vi.fn()} type="date" />, {
      wrapper: Wrapper,
    });

    const input = getInputByName("cutoff_date");

    expect(input.type).toBe("date");
    expect(input.getAttribute("step")).toBeNull();
  });

  it("passes a fully-qualified HH:MM:SS value through unchanged", () => {
    mockParamsDict.cutoff_time = {
      schema: { format: "time", type: "string" },
      value: null,
    };
    const onUpdate = vi.fn();

    render(<FieldDateTime name="cutoff_time" onUpdate={onUpdate} type="time" />, {
      wrapper: Wrapper,
    });

    fireEvent.change(getInputByName("cutoff_time"), { target: { value: "15:58:42" } });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(mockParamsDict.cutoff_time.value).toBe("15:58:42");
    expect(onUpdate).toHaveBeenLastCalledWith("15:58:42");
  });

  it("clears the param to null when the time input is emptied", () => {
    mockParamsDict.cutoff_time = {
      schema: { format: "time", type: "string" },
      value: "15:58:00",
    };
    const onUpdate = vi.fn();

    render(<FieldDateTime name="cutoff_time" onUpdate={onUpdate} type="time" />, {
      wrapper: Wrapper,
    });

    fireEvent.change(getInputByName("cutoff_time"), { target: { value: "" } });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(mockParamsDict.cutoff_time.value).toBeNull();
    expect(onUpdate).toHaveBeenLastCalledWith("");
  });

  it("does not pad date inputs whose value never matches the HH:MM pattern", () => {
    // Guard against the time normalizer leaking into date handling.
    mockParamsDict.cutoff_date = {
      schema: { format: "date", type: "string" },
      value: null,
    };
    const onUpdate = vi.fn();

    render(<FieldDateTime name="cutoff_date" onUpdate={onUpdate} type="date" />, {
      wrapper: Wrapper,
    });

    fireEvent.change(getInputByName("cutoff_date"), { target: { value: "2026-05-28" } });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(mockParamsDict.cutoff_date.value).toBe("2026-05-28");
    expect(onUpdate).toHaveBeenLastCalledWith("2026-05-28");
  });

  it("preserves a previously-stored HH:MM:SS value when re-rendering", () => {
    mockParamsDict.cutoff_time = {
      schema: { format: "time", type: "string" },
      value: "09:15:30",
    };

    render(<FieldDateTime name="cutoff_time" onUpdate={vi.fn()} type="time" />, {
      wrapper: Wrapper,
    });

    expect(getInputByName("cutoff_time").value).toBe("09:15:30");
  });
});
