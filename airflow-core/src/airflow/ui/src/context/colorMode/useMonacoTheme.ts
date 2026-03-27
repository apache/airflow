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
import { useCallback } from "react";

import { useColorMode } from "./useColorMode";

const cssVarToHex = (cssVar: string): string => {
  const value = getComputedStyle(document.documentElement).getPropertyValue(cssVar).trim();
  const canvas = document.createElement("canvas");

  canvas.width = 1;
  canvas.height = 1;
  const ctx = canvas.getContext("2d");

  if (!ctx || !value) {
    return "#000000";
  }
  ctx.fillStyle = value;
  ctx.fillRect(0, 0, 1, 1);
  const [red = 0, green = 0, blue = 0] = ctx.getImageData(0, 0, 1, 1).data;

  return `#${red.toString(16).padStart(2, "0")}${green.toString(16).padStart(2, "0")}${blue.toString(16).padStart(2, "0")}`;
};

const defineMonacoThemes = (monaco: Monaco) => {
  monaco.editor.defineTheme("airflow-light", {
    base: "vs",
    colors: {
      "editor.background": cssVarToHex("--chakra-colors-gray-50"),
      "editor.foreground": cssVarToHex("--chakra-colors-gray-900"),
      "editor.inactiveSelectionBackground": cssVarToHex("--chakra-colors-gray-200"),
      "editor.selectionBackground": cssVarToHex("--chakra-colors-brand-200"),
      "editorGutter.background": cssVarToHex("--chakra-colors-gray-100"),
      "editorLineNumber.activeForeground": cssVarToHex("--chakra-colors-gray-700"),
      "editorLineNumber.foreground": cssVarToHex("--chakra-colors-gray-400"),
      "editorSuggestWidget.background": cssVarToHex("--chakra-colors-gray-50"),
      "editorWidget.background": cssVarToHex("--chakra-colors-gray-50"),
      "editorWidget.border": cssVarToHex("--chakra-colors-gray-300"),
      "scrollbarSlider.background": cssVarToHex("--chakra-colors-gray-300"),
      "scrollbarSlider.hoverBackground": cssVarToHex("--chakra-colors-gray-400"),
    },
    inherit: true,
    rules: [],
  });

  monaco.editor.defineTheme("airflow-dark", {
    base: "vs-dark",
    colors: {
      "editor.background": cssVarToHex("--chakra-colors-gray-900"),
      "editor.foreground": cssVarToHex("--chakra-colors-gray-100"),
      "editor.inactiveSelectionBackground": cssVarToHex("--chakra-colors-gray-800"),
      "editor.selectionBackground": cssVarToHex("--chakra-colors-brand-800"),
      "editorGutter.background": cssVarToHex("--chakra-colors-gray-950"),
      "editorLineNumber.activeForeground": cssVarToHex("--chakra-colors-gray-300"),
      "editorLineNumber.foreground": cssVarToHex("--chakra-colors-gray-500"),
      "editorSuggestWidget.background": cssVarToHex("--chakra-colors-gray-900"),
      "editorWidget.background": cssVarToHex("--chakra-colors-gray-900"),
      "editorWidget.border": cssVarToHex("--chakra-colors-gray-700"),
      "scrollbarSlider.background": cssVarToHex("--chakra-colors-gray-700"),
      "scrollbarSlider.hoverBackground": cssVarToHex("--chakra-colors-gray-600"),
    },
    inherit: true,
    rules: [],
  });
};

export const useMonacoTheme = () => {
  const { colorMode } = useColorMode();

  const beforeMount = useCallback((monaco: Monaco) => {
    defineMonacoThemes(monaco);
  }, []);

  return {
    beforeMount,
    theme: colorMode === "dark" ? "airflow-dark" : "airflow-light",
  };
};
