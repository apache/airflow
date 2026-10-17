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
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { Window as HappyDOMWindow } from "happy-dom";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { SmoothOperatorButton } from "./SmoothOperator";
import { COPY, PHASE_MS } from "./stage";
import { PRIVACY_URL, TERMS_URL } from "./youtube";

const IFRAME_TITLE = "Sade – Smooth Operator (official music video)";

// Lets a test decide how much of the player box is on screen.
let reportVisible: ((ratio: number) => void) | undefined;
let ratioOnObserve = 1;

class FakeIntersectionObserver {
  private readonly callback: IntersectionObserverCallback;

  private isObserving = false;

  public constructor(callback: IntersectionObserverCallback) {
    this.callback = callback;
  }

  public disconnect() {
    this.isObserving = false;
  }

  public observe() {
    this.isObserving = true;
    reportVisible = (ratio) =>
      this.isObserving &&
      this.callback(
        [{ intersectionRatio: ratio, isIntersecting: ratio > 0 } as IntersectionObserverEntry],
        this as unknown as IntersectionObserver,
      );
    reportVisible(ratioOnObserve);
  }
}

const flush = async (ms = 0) => {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
};
const askToOpen = async () => {
  render(<SmoothOperatorButton operatorName="SmoothOperator" taskId="smooth" />, { wrapper: Wrapper });
  fireEvent.click(screen.getByRole("button", { name: COPY.open }));
  await flush();
};
const openScene = async () => {
  await askToOpen();
  fireEvent.click(screen.getByTestId("confirmation-confirm-button"));
  await flush();
};
// The confirmation dialog stays mounted after it closes, so the scene's title is not the only
// level-2 heading on the page; find it by name.
const SCENE_HEADING = { level: 2, name: COPY.title.join(" ") };
const sceneHeading = () => screen.getByRole("heading", SCENE_HEADING);
const querySceneHeading = () => screen.queryByRole("heading", SCENE_HEADING);
// Each phase's timer is only set once React has committed the previous one, so step through them.
const runShow = async () => {
  for (const ms of [PHASE_MS.queued, PHASE_MS.dim, PHASE_MS.needle, PHASE_MS.show]) {
    // eslint-disable-next-line no-await-in-loop
    await flush(ms);
  }
};
// Chakra toggles on the hidden input's change event, so click that rather than the switch root,
// then let React commit the new state before the clock moves on.
const togglePause = async () => {
  fireEvent.click(screen.getByTestId("smooth-pause").querySelector("input") as HTMLInputElement);
  await flush();
};
const getTaskState = (taskId: string) =>
  screen
    .getByTitle(new RegExp(`^${taskId}: `, "u"))
    .getAttribute("title")
    ?.split(": ")[1];

describe("SmoothOperatorButton", () => {
  // Keep happy-dom from fetching the embed; the tests only inspect what the page asks for.
  beforeAll(() => {
    (globalThis as unknown as HappyDOMWindow).happyDOM.settings.navigation.disableChildFrameNavigation = true;
  });

  beforeEach(() => {
    vi.useFakeTimers();
    vi.stubGlobal("IntersectionObserver", FakeIntersectionObserver);
    ratioOnObserve = 1;
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    // A remembered consent lives in localStorage, which outlives a single test.
    localStorage.removeItem("smooth-operator-consent");
  });

  it.each(["BashOperator", null])("renders nothing for %s", (operatorName) => {
    render(<SmoothOperatorButton operatorName={operatorName} taskId="smooth" />, { wrapper: Wrapper });

    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("runs the scene as a Dag and only loads the video once the needle is down", async () => {
    await openScene();

    expect(getTaskState("dim_the_lights")).toMatch(/Queued/u);
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();

    await flush(PHASE_MS.queued);
    expect(getTaskState("dim_the_lights")).toMatch(/Running/u);

    await flush(PHASE_MS.dim);
    expect(getTaskState("dim_the_lights")).toMatch(/Success/u);
    expect(getTaskState("drop_the_needle")).toMatch(/Running/u);
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();

    await flush(PHASE_MS.needle);
    expect(getTaskState("drop_the_needle")).toMatch(/Success/u);
    expect(getTaskState("smooth")).toMatch(/Running/u);

    await flush(PHASE_MS.show);
    expect(getTaskState("smooth")).toMatch(/Success/u);
  });

  it("embeds a sandboxed player and never loads YouTube's API into the page", async () => {
    await openScene();
    await runShow();

    const iframe = screen.getByTitle(IFRAME_TITLE);
    const src = new URL(iframe.getAttribute("src") ?? "");

    expect(src.origin).toBe("https://www.youtube-nocookie.com");
    expect(src.searchParams.get("autoplay")).toBe("1");
    expect(src.searchParams.has("enablejsapi")).toBe(false);
    expect(iframe.getAttribute("sandbox")).toBe(
      "allow-scripts allow-same-origin allow-presentation allow-popups allow-popups-to-escape-sandbox",
    );
    expect(iframe.getAttribute("referrerpolicy")).toBe("strict-origin-when-cross-origin");
    expect(iframe.getAttribute("allow")).toContain("autoplay");
    expect(document.querySelector('script[src*="youtube"]')).toBeNull();
    expect("YT" in globalThis).toBe(false);
  });

  it("does not autoplay the video until more than half of the player is visible", async () => {
    ratioOnObserve = 0;
    await openScene();
    await runShow();

    expect(getTaskState("smooth")).toMatch(/Success/u);
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();

    act(() => reportVisible?.(0.3));
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();

    act(() => reportVisible?.(0.6));
    expect(screen.getByTitle(IFRAME_TITLE)).toBeInTheDocument();
  });

  it.each([
    { init: { key: "Enter" }, skips: true },
    { init: { key: "x" }, skips: true },
    { init: { key: "Tab" }, skips: false },
    { init: { ctrlKey: true, key: "f" }, skips: false },
  ])("routes $init.key during the opening (skips: $skips)", async ({ init, skips }) => {
    const pageShortcut = vi.fn();

    document.addEventListener("keydown", pageShortcut);
    await openScene();
    const event = new KeyboardEvent("keydown", { bubbles: true, cancelable: true, ...init });

    act(() => {
      sceneHeading().dispatchEvent(event);
    });
    document.removeEventListener("keydown", pageShortcut);

    expect(getTaskState("smooth")).toMatch(skips ? /Success/u : /Queued/u);
    // Enter must not also press whatever has focus.
    expect(event.defaultPrevented).toBe(init.key === "Enter");
    // Keys that belong to the scene never reach the task page's shortcuts.
    expect(pageShortcut).toHaveBeenCalledTimes(init.key === "Tab" ? 1 : 0);
  });

  it("asks before opening, and requests nothing from YouTube until confirmed", async () => {
    await askToOpen();

    // Modal spreads its props onto the headless Dialog.Root, so a test id never reaches the DOM;
    // the dialog role is the stable handle, and the scene's own dialog is not mounted yet.
    const dialog = screen.getByRole("dialog");

    expect(dialog).toHaveTextContent(COPY.confirmTitle);
    expect(dialog).toHaveTextContent(COPY.confirmBody);
    for (const [name, href] of [
      [COPY.privacy, PRIVACY_URL],
      [COPY.terms, TERMS_URL],
    ] as const) {
      const link = screen.getByRole("link", { name });

      expect(link).toHaveAttribute("href", href);
      expect(link).toHaveAttribute("target", "_blank");
      expect(link).toHaveAttribute("rel", "noopener noreferrer");
    }
    // The scene, and with it the player, has not been mounted (the dialog's own title is also an h2).
    expect(querySceneHeading()).not.toBeInTheDocument();
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();
    expect(document.querySelector('link[rel="preconnect"], link[rel="dns-prefetch"]')).toBeNull();
  });

  it("opens nothing when the viewer cancels", async () => {
    await askToOpen();

    fireEvent.click(screen.getByTestId("confirmation-cancel-button"));
    await flush();
    await runShow();

    expect(querySceneHeading()).not.toBeInTheDocument();
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();
  });

  it("asks again next time unless the viewer chose to be remembered", async () => {
    await openScene();
    expect(localStorage.getItem("smooth-operator-consent")).toBeNull();
    cleanup();

    // Same browser, next visit: the dialog is back because the box was not ticked.
    await askToOpen();
    expect(screen.getByRole("dialog")).toHaveTextContent(COPY.confirmTitle);
    expect(querySceneHeading()).not.toBeInTheDocument();
  });

  it("skips the dialog next time when the viewer chose to be remembered, and can undo it", async () => {
    await askToOpen();
    fireEvent.click(screen.getByRole("checkbox", { name: COPY.remember }));
    await flush();
    fireEvent.click(screen.getByTestId("confirmation-confirm-button"));
    await flush();
    expect(localStorage.getItem("smooth-operator-consent")).toBe("true");
    cleanup();

    // Next visit: straight to the scene, no dialog.
    await askToOpen();
    expect(screen.queryByTestId("confirmation-confirm-button")).not.toBeInTheDocument();
    expect(sceneHeading()).toBeInTheDocument();

    // The scene offers a way back. Taking it must not count as a click-to-skip (which would load
    // the video), and it closes the scene so nothing more is requested; the dialog returns next time.
    fireEvent.click(screen.getByRole("button", { name: COPY.forget }));
    await flush();
    expect(localStorage.getItem("smooth-operator-consent")).toBeNull();
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();
    await vi.waitFor(() => expect(querySceneHeading()).not.toBeInTheDocument());
  });

  it("activates a button on Enter instead of skipping the opening", async () => {
    await askToOpen();
    fireEvent.click(screen.getByRole("checkbox", { name: COPY.remember }));
    await flush();
    fireEvent.click(screen.getByTestId("confirmation-confirm-button"));
    await flush();

    const forget = screen.getByRole("button", { name: COPY.forget });
    const event = new KeyboardEvent("keydown", { bubbles: true, cancelable: true, key: "Enter" });

    act(() => {
      forget.dispatchEvent(event);
    });

    // Not swallowed by the stage's skip handler: the button keeps its native Enter activation.
    expect(event.defaultPrevented).toBe(false);
    expect(getTaskState("smooth")).toMatch(/Queued/u);
  });

  it("never remembers a cancel, even with the box ticked", async () => {
    await askToOpen();
    fireEvent.click(screen.getByRole("checkbox", { name: COPY.remember }));
    await flush();
    fireEvent.click(screen.getByTestId("confirmation-cancel-button"));
    await flush();

    expect(localStorage.getItem("smooth-operator-consent")).toBeNull();
    expect(querySceneHeading()).not.toBeInTheDocument();
  });

  it("follows a footer link during the opening without skipping or loading the video", async () => {
    await openScene();

    fireEvent.click(screen.getByRole("link", { name: COPY.watch }));
    await flush();

    expect(getTaskState("smooth")).toMatch(/Queued/u);
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();
  });

  it("does not treat a foreign value in storage as consent", async () => {
    // A stored 1 is truthy but is not the true this scene writes.
    localStorage.setItem("smooth-operator-consent", "1");
    await askToOpen();

    expect(screen.getByTestId("confirmation-confirm-button")).toBeInTheDocument();
    expect(querySceneHeading()).not.toBeInTheDocument();
  });

  it("skips to the finished run on a click", async () => {
    await openScene();

    fireEvent.click(sceneHeading());

    expect(getTaskState("smooth")).toMatch(/Success/u);
  });

  it("keeps the player unloaded while its box is still fading in", async () => {
    const realGetComputedStyle = globalThis.getComputedStyle.bind(globalThis);
    let isFading = true;

    vi.spyOn(globalThis, "getComputedStyle").mockImplementation((element, pseudo) => {
      const style = realGetComputedStyle(element, pseudo);

      return isFading
        ? new Proxy(style, {
            get: (target, property) =>
              property === "opacity" ? "0.4" : (Reflect.get(target, property) as unknown),
          })
        : style;
    });
    await openScene();
    await runShow();

    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();

    isFading = false;
    act(() => {
      screen
        .getByTestId("smooth-player")
        .dispatchEvent(Object.assign(new Event("transitionend"), { propertyName: "opacity" }));
    });

    expect(screen.getByTitle(IFRAME_TITLE)).toBeInTheDocument();
  });

  it("closes on Escape, removing the video", async () => {
    await openScene();
    await runShow();

    // The dialog's exit runs on real timers.
    vi.useRealTimers();
    fireEvent.keyDown(sceneHeading(), { key: "Escape" });

    await vi.waitFor(() => expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument());
  });

  it("pauses the Dag where it is and resumes from there", async () => {
    await openScene();
    await flush(PHASE_MS.queued);
    await flush(PHASE_MS.dim / 2);
    expect(getTaskState("dim_the_lights")).toMatch(/Running/u);

    // Pause: the clock stops, however long we wait.
    await togglePause();
    await flush(PHASE_MS.dim * 2);
    expect(getTaskState("dim_the_lights")).toMatch(/Running/u);
    expect(screen.getByLabelText("Unpause Dag")).toBeInTheDocument();

    // Unpause: the phase picks up where it left off and runs only the half it had left.
    await togglePause();
    await flush(PHASE_MS.dim / 2 + 10);
    expect(getTaskState("dim_the_lights")).toMatch(/Success/u);
    expect(getTaskState("drop_the_needle")).toMatch(/Running/u);
  });

  it("still runs the Dag when the viewer prefers reduced motion", async () => {
    vi.stubGlobal("matchMedia", (query: string) => ({ matches: query.includes("reduce") }));
    await openScene();

    expect(getTaskState("dim_the_lights")).toMatch(/Queued/u);
    expect(screen.queryByTitle(IFRAME_TITLE)).not.toBeInTheDocument();

    await runShow();

    expect(getTaskState("smooth")).toMatch(/Success/u);
    expect(screen.getByTitle(IFRAME_TITLE)).toBeInTheDocument();
  });
});
