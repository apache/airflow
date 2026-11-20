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
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { AltClickExtraLinks } from "./AltClickExtraLinks";

const navigate = vi.fn();

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return { ...actual, useNavigate: () => navigate };
});

const navigateTo = "/dags/example_dag/runs/example_run/tasks/example_task";

const renderLinks = (
  links: ReadonlyArray<{ readonly name: string; readonly url: string }>,
  onClick?: () => void,
) =>
  render(
    <AltClickExtraLinks links={links} navigateTo={navigateTo} replace>
      <a data-testid="task" href={navigateTo} onClick={onClick}>
        task
      </a>
    </AltClickExtraLinks>,
    { wrapper: BaseWrapper },
  );

describe("AltClickExtraLinks", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal("open", vi.fn());
  });

  it("opens the only extra link in a new tab and keeps the click from navigating", () => {
    const onClick = vi.fn();

    renderLinks([{ name: "Triggered DAG", url: "https://example.com/run" }], onClick);
    fireEvent.click(screen.getByTestId("task"), { altKey: true, button: 0 });

    expect(globalThis.open).toHaveBeenCalledWith("https://example.com/run", "_blank", "noopener");
    expect(onClick).not.toHaveBeenCalled();
    expect(navigate).not.toHaveBeenCalled();
  });

  it("shows a menu when there are several extra links", () => {
    renderLinks([
      { name: "Triggered DAG", url: "https://example.com/run" },
      { name: "Logs", url: "https://example.com/logs" },
    ]);
    fireEvent.click(screen.getByTestId("task"), { altKey: true, button: 0, clientX: 12, clientY: 34 });

    expect(screen.getByRole("menu")).toBeInTheDocument();
    expect(screen.getByRole("menuitem", { name: "Triggered DAG" })).toHaveAttribute(
      "href",
      "https://example.com/run",
    );
    expect(screen.getByRole("menuitem", { name: "Logs" })).toHaveAttribute(
      "href",
      "https://example.com/logs",
    );
    expect(globalThis.open).not.toHaveBeenCalled();
  });

  it("closes the menu on Escape and on an outside click", () => {
    renderLinks([
      { name: "Triggered DAG", url: "https://example.com/run" },
      { name: "Logs", url: "https://example.com/logs" },
    ]);
    const task = screen.getByTestId("task");

    fireEvent.click(task, { altKey: true, button: 0 });
    fireEvent.keyDown(screen.getByRole("menu"), { key: "Escape" });
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();

    fireEvent.click(task, { altKey: true, button: 0 });
    fireEvent.click(screen.getByTestId("extra-links-backdrop"));
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
  });

  it("navigates to the task instance when there are no extra links", () => {
    renderLinks([]);
    fireEvent.click(screen.getByTestId("task"), { altKey: true, button: 0 });

    expect(globalThis.open).not.toHaveBeenCalled();
    expect(navigate).toHaveBeenCalledWith(navigateTo, { replace: true });
  });

  it("leaves ordinary and non-left clicks alone", () => {
    const onClick = vi.fn();

    renderLinks([{ name: "Triggered DAG", url: "https://example.com/run" }], onClick);
    fireEvent.click(screen.getByTestId("task"), { button: 0 });
    fireEvent.click(screen.getByTestId("task"), { altKey: true, button: 1 });

    expect(onClick).toHaveBeenCalledTimes(2);
    expect(globalThis.open).not.toHaveBeenCalled();
    expect(navigate).not.toHaveBeenCalled();
  });
});
