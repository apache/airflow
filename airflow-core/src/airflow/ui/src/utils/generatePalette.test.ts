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
import { describe, it, expect } from "vitest";

import {
  roundTo3rdDecimal,
  quadraticLagrangeInterpolatingPolynomial,
  generatePalette,
} from "./generatePalette";

describe("roundTo3rdDecimal", () => {
  const roundTestCases = [
    [0, 0],
    [1.5, 1.5],
    [9.8456, 9.846],
    [215_415.098_704_36, 215_415.099],
    [-15.4564, -15.456],
    [-8.9265, -8.926],
  ];

  it.each(roundTestCases)("round to 3rd decimal %d", (number, expected) => {
    expect(roundTo3rdDecimal(number)).toBe(expected);
  });
});

describe("quadraticLagrangeInterpolatingPolynomial", () => {
  it("interpolate three points", () => {
    // f(x) = 2x^2 - 5x + 1
    const point0: [number, number] = [0, 1];
    const point1: [number, number] = [3, 4];
    const point2: [number, number] = [5, 26];

    // f(2)
    expect(quadraticLagrangeInterpolatingPolynomial(point0, point1, point2)(2)).toBe(-1);
  });

  it("order of points does not matter", () => {
    // f(x) = x^2
    const point0: [number, number] = [-2, 4];
    const point1: [number, number] = [1, 1];
    const point2: [number, number] = [3, 9];

    // f(2)
    expect(quadraticLagrangeInterpolatingPolynomial(point1, point0, point2)(2)).toBe(4);
  });
});

describe("generatePalette", () => {
  const whiteBlackPalette = {
    "50": { value: "oklch(0.97 0 0)" },
    "100": { value: "oklch(0.932 0 0)" },
    "200": { value: "oklch(0.857 0 0)" },
    "300": { value: "oklch(0.781 0 0)" },
    "400": { value: "oklch(0.706 0 0)" },
    "500": { value: "oklch(0.63 0 0)" },
    "600": { value: "oklch(0.554 0 0)" },
    "700": { value: "oklch(0.479 0 0)" },
    "800": { value: "oklch(0.403 0 0)" },
    "900": { value: "oklch(0.328 0 0)" },
    "950": { value: "oklch(0.29 0 0)" },
  };

  const paletteTestCase = [
    {
      color: "#ffffff",
      palette: whiteBlackPalette,
    },
    {
      color: "#000000",
      palette: whiteBlackPalette,
    },
    {
      color: "#cdd5d2",
      palette: {
        "50": { value: "oklch(0.97 0.005 171.759)" },
        "100": { value: "oklch(0.932 0.006 171.759)" },
        "200": { value: "oklch(0.857 0.008 171.759)" },
        "300": { value: "oklch(0.781 0.009 171.759)" },
        "400": { value: "oklch(0.706 0.01 171.759)" },
        "500": { value: "oklch(0.63 0.01 171.759)" },
        "600": { value: "oklch(0.554 0.01 171.759)" },
        "700": { value: "oklch(0.479 0.009 171.759)" },
        "800": { value: "oklch(0.403 0.008 171.759)" },
        "900": { value: "oklch(0.328 0.006 171.759)" },
        "950": { value: "oklch(0.29 0.005 171.759)" },
      },
    },
  ];

  it("wrong hex", () => {
    expect(generatePalette("#ttgh12")).toBe(undefined);
  });

  it("no six-digit form hex ", () => {
    expect(generatePalette("#f12")).toBe(undefined);
  });

  it.each(paletteTestCase)("generate palette for $color", ({ color, palette }) => {
    expect(generatePalette(color)).toStrictEqual(palette);
  });
});
