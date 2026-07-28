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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { UIAlert } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { AlertContent } from "./AlertContent";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) =>
      key === "alerts.seeMoreContext" ? "See more" : key === "alerts.seeLessContext" ? "See less" : key,
  }),
}));

// happy-dom reports 0 for all layout dimensions, so overflow is driven by
// stubbing scrollHeight and clientHeight to simulate clamped vs. full height.
const CLAMPED_HEIGHT = 100;
const OVERFLOW_HEIGHT = 200;

const stubScrollHeight = (height: number) => {
  Object.defineProperty(HTMLElement.prototype, "scrollHeight", {
    configurable: true,
    get: () => height,
  });
};

const stubClientHeight = (height: number) => {
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get: () => height,
  });
};

const renderAlert = (alert: UIAlert) => render(<AlertContent alert={alert} />, { wrapper: Wrapper });

describe("AlertContent", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "ResizeObserver",
      class MockResizeObserver {
        // eslint-disable-next-line @typescript-eslint/class-methods-use-this
        public disconnect() {
          /* empty */
        }
        // eslint-disable-next-line @typescript-eslint/class-methods-use-this
        public observe() {
          /* empty */
        }
      },
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    Reflect.deleteProperty(HTMLElement.prototype, "scrollHeight");
    Reflect.deleteProperty(HTMLElement.prototype, "clientHeight");
  });

  it("shows no See more toggle when the alert fits within the clamp", () => {
    stubScrollHeight(CLAMPED_HEIGHT);
    stubClientHeight(CLAMPED_HEIGHT);

    renderAlert({ category: "info", text: "Short alert text" });

    expect(screen.getByText("Short alert text")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "See more" })).not.toBeInTheDocument();
  });

  it("shows no See more when content exceeds clientHeight but still fits the clamp", () => {
    // Regression: a single line of inline content (bold, inline code) leaves scrollHeight
    // a few px above clientHeight. Overflow must be decided by the clamp height, not by
    // scrollHeight > clientHeight, which would surface a See more toggle with nothing to reveal.
    stubScrollHeight(CLAMPED_HEIGHT + 10);
    stubClientHeight(CLAMPED_HEIGHT);

    renderAlert({ category: "info", text: "Short alert text" });

    expect(screen.queryByRole("button", { name: "See more" })).not.toBeInTheDocument();
  });

  it("clamps a tall alert and toggles between See more and See less", () => {
    stubScrollHeight(OVERFLOW_HEIGHT);
    stubClientHeight(CLAMPED_HEIGHT);

    renderAlert({ category: "info", text: "Long alert text" });

    const seeMore = screen.getByRole("button", { name: "See more" });

    expect(seeMore).toBeVisible();
    expect(screen.queryByRole("button", { name: "See less" })).not.toBeInTheDocument();

    fireEvent.click(seeMore);

    expect(screen.getByRole("button", { name: "See less" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "See more" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "See less" }));

    expect(screen.getByRole("button", { name: "See more" })).toBeVisible();
  });

  it("stops the toggle click from bubbling to a surrounding accordion trigger", () => {
    stubScrollHeight(OVERFLOW_HEIGHT);
    stubClientHeight(CLAMPED_HEIGHT);

    const onParentClick = vi.fn();

    render(
      // eslint-disable-next-line jsx-a11y/click-events-have-key-events, jsx-a11y/no-static-element-interactions
      <div onClick={onParentClick}>
        <AlertContent alert={{ category: "info", text: "Long alert text" }} />
      </div>,
      { wrapper: Wrapper },
    );

    fireEvent.click(screen.getByRole("button", { name: "See more" }));

    expect(screen.getByRole("button", { name: "See less" })).toBeVisible();
    expect(onParentClick).not.toHaveBeenCalled();
  });
});
