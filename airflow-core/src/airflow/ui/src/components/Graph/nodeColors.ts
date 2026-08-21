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

// The theme's default text colors ("fg"): black in light mode, gray.50 in dark mode. We pick between
// them by the fill brightness instead of by the color mode, so custom-colored nodes stay legible.
const DARK_TEXT = "black";
const LIGHT_TEXT = "gray.50";
const BRIGHTNESS_THRESHOLD = 128;

const hexToRgb = (hex: string): [number, number, number] | undefined => {
  const isShort = /^#[\da-f]{3}$/iu.test(hex);
  const isLong = /^#[\da-f]{6}$/iu.test(hex);

  if (!isShort && !isLong) {
    return undefined;
  }

  // Expand #abc to #aabbcc so both forms share one slicing path.
  const normalized = isShort
    ? `#${hex.slice(1, 2).repeat(2)}${hex.slice(2, 3).repeat(2)}${hex.slice(3, 4).repeat(2)}`
    : hex;

  return [
    parseInt(normalized.slice(1, 3), 16),
    parseInt(normalized.slice(3, 5), 16),
    parseInt(normalized.slice(5, 7), 16),
  ];
};

// Perceived brightness (ITU-R BT.601, 0-255) of a raw color -- a hex code parsed directly, or a CSS
// name normalized through a canvas. Returns undefined for anything that cannot be resolved.
const perceivedBrightness = (color: string): number | undefined => {
  let rgb = hexToRgb(color);

  if (rgb === undefined) {
    const context = document.createElement("canvas").getContext("2d");

    if (context !== null) {
      context.fillStyle = "#000000";
      context.fillStyle = color;
      rgb = hexToRgb(context.fillStyle);
    }
  }

  if (rgb === undefined) {
    return undefined;
  }

  const [red, green, blue] = rgb;

  return (red * 299 + green * 587 + blue * 114) / 1000;
};

// Pick the theme's dark or light text color for a node painted with the vivid fill `color`, so the
// icon and label stay legible. Chakra palette tokens ("blue.500") are theme-managed and keep the
// default foreground (undefined), as do colors that cannot be parsed.
export const readableTextForFill = (color: string | undefined): string | undefined => {
  if (color === undefined || color.includes(".")) {
    return undefined;
  }

  const brightness = perceivedBrightness(color);

  if (brightness === undefined) {
    return undefined;
  }

  return brightness >= BRIGHTNESS_THRESHOLD ? DARK_TEXT : LIGHT_TEXT;
};
