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
import type { Monaco } from "@monaco-editor/react";
import { formatHex, parse } from "culori";

import { useColorMode } from "./useColorMode";

const LIGHT_THEME_NAME = "airflow-light";
const DARK_THEME_NAME = "airflow-dark";

let themesRegistered = false;

// Convert any CSS color (including modern color spaces like OKLCH that Chakra
// UI uses) to a #rrggbb string that Monaco's `defineTheme` accepts. culori
// handles parsing and gamut mapping; we fall back to black for unset or
// unparsable values so Monaco never sees an invalid color.
const toHex = (cssVar: string): string => {
  const value = getComputedStyle(document.documentElement).getPropertyValue(cssVar).trim();

  return formatHex(parse(value)) ?? "#000000";
};

const defineAirflowMonacoThemes = (monaco: Monaco) => {
  if (themesRegistered) {
    return;
  }

  monaco.editor.defineTheme(LIGHT_THEME_NAME, {
    base: "vs",
    colors: {
      "editor.background": toHex("--chakra-colors-gray-50"),
      "editor.foreground": toHex("--chakra-colors-gray-900"),
      "editor.inactiveSelectionBackground": toHex("--chakra-colors-gray-200"),
      "editor.lineHighlightBackground": toHex("--chakra-colors-gray-100"),
      "editor.selectionBackground": toHex("--chakra-colors-brand-200"),
      "editorGutter.background": toHex("--chakra-colors-gray-50"),
      "editorLineNumber.activeForeground": toHex("--chakra-colors-gray-700"),
      "editorLineNumber.foreground": toHex("--chakra-colors-gray-400"),
      "editorSuggestWidget.background": toHex("--chakra-colors-gray-50"),
      "editorWidget.background": toHex("--chakra-colors-gray-50"),
      "editorWidget.border": toHex("--chakra-colors-gray-300"),
      "scrollbarSlider.activeBackground": `${toHex("--chakra-colors-gray-500")}c0`,
      "scrollbarSlider.background": `${toHex("--chakra-colors-gray-300")}80`,
      "scrollbarSlider.hoverBackground": `${toHex("--chakra-colors-gray-400")}a0`,
    },
    inherit: true,
    rules: [],
  });

  monaco.editor.defineTheme(DARK_THEME_NAME, {
    base: "vs-dark",
    colors: {
      "editor.background": toHex("--chakra-colors-gray-900"),
      "editor.foreground": toHex("--chakra-colors-gray-100"),
      "editor.inactiveSelectionBackground": toHex("--chakra-colors-gray-800"),
      "editor.lineHighlightBackground": toHex("--chakra-colors-gray-800"),
      "editor.selectionBackground": toHex("--chakra-colors-brand-800"),
      "editorGutter.background": toHex("--chakra-colors-gray-900"),
      "editorLineNumber.activeForeground": toHex("--chakra-colors-gray-300"),
      "editorLineNumber.foreground": toHex("--chakra-colors-gray-600"),
      "editorSuggestWidget.background": toHex("--chakra-colors-gray-900"),
      "editorWidget.background": toHex("--chakra-colors-gray-900"),
      "editorWidget.border": toHex("--chakra-colors-gray-700"),
      "scrollbarSlider.activeBackground": `${toHex("--chakra-colors-gray-500")}c0`,
      "scrollbarSlider.background": `${toHex("--chakra-colors-gray-700")}80`,
      "scrollbarSlider.hoverBackground": `${toHex("--chakra-colors-gray-600")}a0`,
    },
    inherit: true,
    rules: [],
  });

  themesRegistered = true;
};

export const useMonacoTheme = () => {
  const { colorMode } = useColorMode();

  return {
    beforeMount: defineAirflowMonacoThemes,
    theme: colorMode === "dark" ? DARK_THEME_NAME : LIGHT_THEME_NAME,
  };
};
