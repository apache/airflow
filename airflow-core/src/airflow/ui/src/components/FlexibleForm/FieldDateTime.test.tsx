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
import { beforeEach, describe, expect, it, vi } from "vitest";

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

describe("FieldDateTime", () => {
  beforeEach(() => {
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
    mockSetParamsDict.mockClear();
  });

  describe("type=time (format:time)", () => {
    it("renders with step=1 so the browser shows the seconds field", () => {
      mockParamsDict.cutoff_time = { schema: { format: "time", type: "string" }, value: "" };

      const { container } = render(
        <FieldDateTime name="cutoff_time" onUpdate={vi.fn()} type="time" />,
        { wrapper: Wrapper },
      );

      const input = container.querySelector("input") as HTMLInputElement;

      expect(input.step).toBe("1");
    });

    it("pads HH:MM to HH:MM:SS when the user enters a time without seconds", () => {
      mockParamsDict.cutoff_time = { schema: { format: "time", type: "string" }, value: "" };
      const onUpdate = vi.fn();

      const { container } = render(
        <FieldDateTime name="cutoff_time" onUpdate={onUpdate} type="time" />,
        { wrapper: Wrapper },
      );

      const input = container.querySelector("input") as HTMLInputElement;

      fireEvent.change(input, { target: { value: "19:30" } });

      expect(onUpdate).toHaveBeenCalledWith("19:30:00");
    });

    it("does not double-pad when value already contains seconds", () => {
      mockParamsDict.cutoff_time = { schema: { format: "time", type: "string" }, value: "" };
      const onUpdate = vi.fn();

      const { container } = render(
        <FieldDateTime name="cutoff_time" onUpdate={onUpdate} type="time" />,
        { wrapper: Wrapper },
      );

      const input = container.querySelector("input") as HTMLInputElement;

      fireEvent.change(input, { target: { value: "19:30:45" } });

      expect(onUpdate).toHaveBeenCalledWith("19:30:45");
    });

    it("passes an empty value through unmodified", () => {
      mockParamsDict.cutoff_time = { schema: { format: "time", type: "string" }, value: "10:00:00" };
      const onUpdate = vi.fn();

      const { container } = render(
        <FieldDateTime name="cutoff_time" onUpdate={onUpdate} type="time" />,
        { wrapper: Wrapper },
      );

      const input = container.querySelector("input") as HTMLInputElement;

      fireEvent.change(input, { target: { value: "" } });

      expect(onUpdate).toHaveBeenCalledWith("");
    });
  });

  describe("type=date (format:date)", () => {
    it("renders without a step attribute", () => {
      mockParamsDict.my_date = { schema: { format: "date", type: "string" }, value: "" };

      const { container } = render(
        <FieldDateTime name="my_date" onUpdate={vi.fn()} type="date" />,
        { wrapper: Wrapper },
      );

      const input = container.querySelector("input") as HTMLInputElement;

      expect(input.step).toBe("");
    });

    it("passes the typed value through without modification", () => {
      mockParamsDict.my_date = { schema: { format: "date", type: "string" }, value: "" };
      const onUpdate = vi.fn();

      const { container } = render(
        <FieldDateTime name="my_date" onUpdate={onUpdate} type="date" />,
        { wrapper: Wrapper },
      );

      const input = container.querySelector("input") as HTMLInputElement;

      fireEvent.change(input, { target: { value: "2026-05-07" } });

      expect(onUpdate).toHaveBeenCalledWith("2026-05-07");
    });
  });
});