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
import { oklch } from "culori";

export type Palette = Record<string, { value: string }>;

export const roundTo3rdDecimal = (number: number) => {
  const factor = 10 ** 3;

  return Math.round((number + Number.EPSILON) * factor) / factor;
};

export const quadraticLagrangeInterpolatingPolynomial = (
  point0: [number, number],
  point1: [number, number],
  point2: [number, number],
) => {
  // For mathematical background see https://en.wikipedia.org/wiki/Lagrange_polynomial
  const base0 = (value: number) =>
    ((value - point1[0]) / (point0[0] - point1[0])) * ((value - point2[0]) / (point0[0] - point2[0]));
  const base1 = (value: number) =>
    ((value - point0[0]) / (point1[0] - point0[0])) * ((value - point2[0]) / (point1[0] - point2[0]));
  const base2 = (value: number) =>
    ((value - point1[0]) / (point2[0] - point1[0])) * ((value - point0[0]) / (point2[0] - point0[0]));

  const linearCombinationBasis = (value: number) =>
    base0(value) * point0[1] + base1(value) * point1[1] + base2(value) * point2[1];

  return linearCombinationBasis;
};

export const generatePalette = (hex: string): Palette | undefined => {
  if (!/^#[\da-f]{6}$/iu.test(hex)) {
    return undefined;
  }

  const base = oklch(hex);

  if (!base) {
    return undefined;
  }

  let { c: chroma = 0, h: hue = 0 } = base;

  chroma = roundTo3rdDecimal(chroma);
  hue = roundTo3rdDecimal(hue);

  const keys = [50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 950];

  const lightnessSteps = [0.97, 0.932, 0.857, 0.781, 0.706, 0.63, 0.554, 0.479, 0.403, 0.328, 0.29];

  const chroma50 = chroma < 0.013 ? roundTo3rdDecimal(chroma / 2) : 0.013;
  const chroma950 = chroma < 0.048 ? roundTo3rdDecimal(chroma / 2) : 0.048;
  const chromaSteps =
    chroma === 0
      ? () => chroma
      : quadraticLagrangeInterpolatingPolynomial([50, chroma50], [500, chroma], [950, chroma950]);

  const palette: Palette = {};

  keys.forEach((key, position) => {
    const lightness = lightnessSteps[position];
    const newChroma = roundTo3rdDecimal(chromaSteps(key));
    const color = { value: `oklch(${lightness} ${newChroma} ${hue})` };

    palette[key.toString()] = color;
  });

  return palette;
};
