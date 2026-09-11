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
import { act, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { useNearViewport } from "./useNearViewport";

type MockObserver = {
  callback: IntersectionObserverCallback;
} & IntersectionObserver;

const observers: Array<MockObserver> = [];

const createEntry = (target: Element, isIntersecting: boolean): IntersectionObserverEntry => ({
  boundingClientRect: target.getBoundingClientRect(),
  intersectionRatio: isIntersecting ? 1 : 0,
  intersectionRect: target.getBoundingClientRect(),
  isIntersecting,
  rootBounds: null,
  target,
  time: 0,
});

const takeNoRecords = () => [];

const MockIntersectionObserver = function MockIntersectionObserver(
  callback: IntersectionObserverCallback,
  options?: IntersectionObserverInit,
): IntersectionObserver {
  const observer = {
    callback,
    disconnect: vi.fn(),
    observe: vi.fn(),
    root: options?.root ?? null,
    rootMargin: options?.rootMargin ?? "0px",
    scrollMargin: options?.scrollMargin ?? "0px",
    takeRecords: vi.fn(takeNoRecords),
    thresholds: [0],
    unobserve: vi.fn(),
  } satisfies MockObserver;

  observers.push(observer);

  return observer;
};

const installIntersectionObserver = () => {
  vi.stubGlobal("IntersectionObserver", MockIntersectionObserver);
};

const TestCard = ({ name }: { readonly name: string }) => {
  const { isNearViewport, ref } = useNearViewport<HTMLDivElement>();

  return (
    <div data-testid={`${name}-shell`} ref={ref}>
      {name}
      {isNearViewport ? <span>{`${name} controls`}</span> : <span>{`${name} placeholder`}</span>}
    </div>
  );
};

afterEach(() => {
  observers.length = 0;
  vi.unstubAllGlobals();
});

describe("useNearViewport", () => {
  it("mounts deferred content when its shell approaches the viewport and keeps it mounted", () => {
    installIntersectionObserver();
    render(<TestCard name="first" />);

    expect(screen.getByTestId("first-shell")).toBeInTheDocument();
    expect(screen.getByText("first placeholder")).toBeInTheDocument();
    expect(screen.queryByText("first controls")).not.toBeInTheDocument();
    expect(observers).toHaveLength(1);
    expect(observers[0]?.rootMargin).toBe("600px 0px");
    expect(observers[0]?.scrollMargin).toBe("0px");

    const [observer] = observers;
    const shell = screen.getByTestId("first-shell");

    if (observer === undefined) {
      throw new Error("Expected an IntersectionObserver");
    }

    act(() => {
      observer.callback([createEntry(shell, true)], observer);
    });

    expect(screen.getByText("first controls")).toBeInTheDocument();
    expect(screen.queryByText("first placeholder")).not.toBeInTheDocument();
    expect(observer.unobserve).toHaveBeenCalledWith(shell);

    act(() => {
      observer.callback([createEntry(shell, false)], observer);
    });

    expect(screen.getByText("first controls")).toBeInTheDocument();
  });

  it("shares one observer across multiple shells", () => {
    installIntersectionObserver();
    render(
      <>
        <TestCard name="first" />
        <TestCard name="second" />
      </>,
    );

    expect(observers).toHaveLength(1);
    expect(observers[0]?.observe).toHaveBeenCalledTimes(2);

    const [observer] = observers;
    const secondShell = screen.getByTestId("second-shell");

    if (observer === undefined) {
      throw new Error("Expected an IntersectionObserver");
    }

    act(() => {
      observer.callback([createEntry(secondShell, true)], observer);
    });

    expect(screen.getByText("first placeholder")).toBeInTheDocument();
    expect(screen.getByText("second controls")).toBeInTheDocument();
  });

  it("uses the nearest scroll container as the preload root", () => {
    installIntersectionObserver();
    render(
      <div
        data-testid="scroll-root"
        ref={(element) => {
          if (element !== null) {
            Object.defineProperties(element, {
              clientHeight: { configurable: true, value: 100 },
              scrollHeight: { configurable: true, value: 1000 },
            });
          }
        }}
        style={{ overflowY: "auto" }}
      >
        <TestCard name="first" />
      </div>,
    );

    expect(observers).toHaveLength(1);
    expect(observers[0]?.root).toBe(screen.getByTestId("scroll-root"));
    expect(observers[0]?.rootMargin).toBe("600px 0px");
  });

  it("skips an overflow wrapper that does not actually scroll", () => {
    installIntersectionObserver();
    render(
      <div
        data-testid="scroll-root"
        ref={(element) => {
          if (element !== null) {
            Object.defineProperties(element, {
              clientHeight: { configurable: true, value: 100 },
              scrollHeight: { configurable: true, value: 1000 },
            });
          }
        }}
        style={{ overflowY: "auto" }}
      >
        <div data-testid="overflow-wrapper" style={{ overflowY: "auto" }}>
          <TestCard name="first" />
        </div>
      </div>,
    );

    expect(observers).toHaveLength(1);
    expect(observers[0]?.root).toBe(screen.getByTestId("scroll-root"));
    expect(observers[0]?.root).not.toBe(screen.getByTestId("overflow-wrapper"));
  });

  it("unobserves a pending shell and releases the shared observer on unmount", () => {
    installIntersectionObserver();
    const { unmount } = render(<TestCard name="first" />);

    const [observer] = observers;
    const shell = screen.getByTestId("first-shell");

    if (observer === undefined) {
      throw new Error("Expected an IntersectionObserver");
    }

    unmount();

    expect(observer.unobserve).toHaveBeenCalledWith(shell);
    expect(observer.disconnect).toHaveBeenCalledOnce();
  });

  it("mounts content when IntersectionObserver is unavailable", async () => {
    vi.stubGlobal("IntersectionObserver", undefined);
    render(<TestCard name="first" />);

    expect(await screen.findByText("first controls")).toBeInTheDocument();
  });
});
