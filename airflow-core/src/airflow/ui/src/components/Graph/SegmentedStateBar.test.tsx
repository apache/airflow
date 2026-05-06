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
import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { SegmentedStateBar } from "./SegmentedStateBar";

describe("SegmentedStateBar", () => {
  it("renders nothing when childStates is null and no fallbackState", () => {
    const { container } = render(<SegmentedStateBar childStates={null} />, {
      wrapper: Wrapper,
    });

    expect(container.innerHTML).toBe("");
  });

  it("renders a single solid bar when childStates is null with fallbackState", () => {
    const { container } = render(<SegmentedStateBar childStates={null} fallbackState="running" />, {
      wrapper: Wrapper,
    });

    const boxes = container.querySelectorAll("[class]");

    // Should render a single box element (the fallback bar)
    expect(boxes.length).toBeGreaterThan(0);
    // Should not contain a flex container with multiple children
    const flexContainer = container.querySelector("[style*='flex']");

    expect(flexContainer).toBeNull();
  });

  it("renders proportional segments for mixed states", () => {
    const childStates = { running: 3, scheduled: 4, success: 1 };
    const { container } = render(<SegmentedStateBar childStates={childStates} />, {
      wrapper: Wrapper,
    });

    // The flex container should have exactly 3 child segments (one per non-zero state)
    const flexContainer = container.firstChild;

    expect(flexContainer?.childNodes.length).toBe(3);
  });

  it("excludes zero-count states from segments", () => {
    const childStates = { failed: 0, running: 2, success: 0 };
    const { container } = render(<SegmentedStateBar childStates={childStates} />, {
      wrapper: Wrapper,
    });

    // Only "running" has count > 0, so exactly 1 segment child
    const flexContainer = container.firstChild;

    expect(flexContainer?.childNodes.length).toBe(1);
  });

  it("renders nothing when childStates is empty object and no fallbackState", () => {
    const { container } = render(<SegmentedStateBar childStates={{}} />, {
      wrapper: Wrapper,
    });

    expect(container.innerHTML).toBe("");
  });
});
