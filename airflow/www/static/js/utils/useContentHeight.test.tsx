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

/* global document, window */

import React from "react";
import { renderHook } from "@testing-library/react";

import useContentHeight from "./useContentHeight";

const viewportHeight = 900;
const footerReserve = 70;

const refAt = (viewportTop: number) =>
  ({
    current: {
      getBoundingClientRect: () => ({ top: viewportTop }),
    },
  }) as unknown as React.RefObject<HTMLElement>;

const scrollTo = (scrollY: number) =>
  Object.defineProperty(window, "scrollY", { value: scrollY, writable: true });

describe("Test useContentHeight", () => {
  beforeEach(() => {
    window.innerHeight = viewportHeight;
    document.body.style.paddingBottom = `${footerReserve}px`;
    scrollTo(0);
  });

  test("Fills the viewport below the element, leaving the footer reserve", () => {
    const { result } = renderHook(() => useContentHeight(refAt(180)));

    expect(result.current).toBe(viewportHeight - footerReserve - 180);
  });

  test("Ignores the page scroll position", () => {
    // Same element, but the page is scrolled down 200px so it sits higher in the viewport.
    scrollTo(200);
    const { result } = renderHook(() => useContentHeight(refAt(180 - 200)));

    expect(result.current).toBe(viewportHeight - footerReserve - 180);
  });

  test("Never returns a negative height", () => {
    const { result } = renderHook(() =>
      useContentHeight(refAt(viewportHeight)),
    );

    expect(result.current).toBe(0);
  });
});
