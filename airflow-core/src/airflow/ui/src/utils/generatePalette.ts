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

export const generatePalette = (hex: string): Palette | undefined => {
  if (!/^#[\da-f]{6}$/iu.test(hex)) {
    return undefined;
  }

  const base = oklch(hex);

  if (!base) {
    return undefined;
  }

  const { c: chroma = 0, h: hue = 0 } = base;

  const lightnessSteps = [0.97, 0.95, 0.89, 0.82, 0.71, 0.6, 0.48, 0.36, 0.27, 0.18, 0.15];

  const keys = ["50", "100", "200", "300", "400", "500", "600", "700", "800", "900", "950"];

  const palette: Palette = {};

  keys.forEach((key, position) => {
    const lightness = lightnessSteps[position];
    const color = { value: `oklch(${lightness} ${chroma} ${hue})` };

    palette[key] = color;
  });

  return palette;
};
