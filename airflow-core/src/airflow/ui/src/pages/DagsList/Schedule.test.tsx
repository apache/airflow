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
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { Schedule } from "./Schedule";

describe("Schedule", () => {
  it("renders the timetable description tooltip outside the cell, so the cell's nowrap does not apply", async () => {
    vi.useFakeTimers();

    try {
      const { container } = render(
        <Schedule
          assetExpression={undefined}
          dagId="my_dag"
          timetableDescription="At 09:00, Monday through Friday"
          timetablePartitioned={false}
          timetableSummary="0 9 * * 1-5"
        />,
        { wrapper: BaseWrapper },
      );
      const trigger = screen.getByText("0 9 * * 1-5");

      await act(async () => {
        fireEvent.focus(trigger);
        fireEvent.pointerEnter(trigger);
        await vi.advanceTimersByTimeAsync(500);
      });

      const tooltip = screen.getByRole("tooltip");

      expect(tooltip).toHaveTextContent("At 09:00, Monday through Friday");
      expect(container).not.toContainElement(tooltip);
    } finally {
      vi.useRealTimers();
    }
  });
});
