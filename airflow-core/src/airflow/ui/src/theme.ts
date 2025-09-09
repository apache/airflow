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

/* eslint-disable perfectionist/sort-objects */

/* eslint-disable max-lines */
import { createSystem, defaultConfig, defineConfig } from "@chakra-ui/react";
import type { CSSProperties } from "react";

const generateSemanticTokens = (color: string, darkContrast: string = "white") => ({
  solid: { value: `{colors.${color}.600}` },
  contrast: { value: { _light: "white", _dark: darkContrast } },
  fg: { value: { _light: `{colors.${color}.800}`, _dark: `{colors.${color}.200}` } },
  muted: { value: { _light: `{colors.${color}.200}`, _dark: `{colors.${color}.800}` } },
  subtle: { value: { _light: `{colors.${color}.100}`, _dark: `{colors.${color}.900}` } },
  emphasized: { value: { _light: `{colors.${color}.300}`, _dark: `{colors.${color}.700}` } },
  focusRing: { value: { _light: `{colors.${color}.800}`, _dark: `{colors.${color}.200}` } },
});

export const customConfig = defineConfig({
  // See https://chakra-ui.com/docs/theming/colors for more information on the colors used here.
  theme: {
    tokens: {
      colors: {
        black: { value: "oklch(0.23185 0.0323 266.44)" }, // Custom value for dark mode
        brand: {
          "50": { value: "oklch(0.98 0.006 248.717)" },
          "100": { value: "oklch(0.962 0.012 249.460)" },
          "200": { value: "oklch(0.923 0.023 255.082)" },
          "300": { value: "oklch(0.865 0.039 252.420)" },
          "400": { value: "oklch(0.705 0.066 256.378)" },
          "500": { value: "oklch(0.575 0.08 257.759)" },
          "600": { value: "oklch(0.469 0.084 257.657)" },
          "700": { value: "oklch(0.399 0.084 257.850)" },
          "800": { value: "oklch(0.324 0.072 260.329)" },
          "900": { value: "oklch(0.259 0.062 265.566)" },
          "950": { value: "oklch(0.179 0.05 265.487)" },
        },
        gray: {
          // Values modified from original Tailwind to improve contrast in Chakra UI
          "50": { value: "oklch(0.985 0.004 253)" }, // Original: oklch(0.985 0.002 247.839)
          "100": { value: "oklch(0.955 0.006 253)" }, // Original: oklch(0.967 0.003 264.542)
          "200": { value: "oklch(0.915 0.01 253)" }, // Original: oklch(0.928 0.006 264.531)
          "300": { value: "oklch(0.85 0.016 253)" }, // Original: oklch(0.872 0.01 258.338)
          "400": { value: "oklch(0.75 0.025 252)" }, // Original: oklch(0.707 0.022 261.325)
          "500": { value: "oklch(0.63, 0.04, 252)" }, // Original: oklch(0.551 0.027 264.364)
          "600": { value: "oklch(0.45 0.055 251)" }, // Original: oklch(0.446 0.03 256.802)
          "700": { value: "oklch(0.35 0.045 251)" }, // Original: oklch(0.373 0.034 259.733)
          "800": { value: "oklch(0.28 0.035 251)" }, // Original: oklch(0.278 0.033 256.848)
          "900": { value: "oklch(0.18 0.03 251)" }, // Original: oklch(0.21 0.034 264.665)
          "950": { value: "oklch(0.11 0.025 251)" }, // Original: oklch(0.13 0.028 261.692)
        },
        // TAILWIND 4.0 COLORS
        // See https://tailwindcss.com/docs/colors for more information on the colors used here.
        red: {
          "50": { value: "oklch(0.971 0.013 17.38)" },
          "100": { value: "oklch(0.936 0.032 17.717)" },
          "200": { value: "oklch(0.885 0.062 18.334)" },
          "300": { value: "oklch(0.808 0.114 19.571)" },
          "400": { value: "oklch(0.704 0.191 22.216)" },
          "500": { value: "oklch(0.637 0.237 25.331)" },
          "600": { value: "oklch(0.577 0.245 27.325)" },
          "700": { value: "oklch(0.505 0.213 27.518)" },
          "800": { value: "oklch(0.444 0.177 26.899)" },
          "900": { value: "oklch(0.396 0.141 25.723)" },
          "950": { value: "oklch(0.258 0.092 26.042)" },
        },
        // Values modified from original Tailwind to improve contrast in Chakra UI
        orange: {
          "50": { value: "oklch(0.982 0.013 83.915)" },
          "100": { value: "oklch(0.961 0.033 82.320)" },
          "200": { value: "oklch(0.918 0.065 79.975)" },
          "300": { value: "oklch(0.857 0.118 76.815)" },
          "400": { value: "oklch(0.7492 0.1439 62.081)" }, // Original: oklch(0.774 0.186 71.555)
          "500": { value: "oklch(0.6462 0.1979 43.792)" }, // Original: oklch(0.705 0.213 47.604)
          "600": { value: "oklch(0.5902 0.198 35.93)" }, // Original: oklch(0.632 0.214 41.185)
          "700": { value: "oklch(0.553 0.184 41.777)" },
          "800": { value: "oklch(0.469 0.144 45.164)" },
          "900": { value: "oklch(0.414 0.110 48.717)" },
          "950": { value: "oklch(0.271 0.069 52.345)" },
        },
        amber: {
          "50": { value: "oklch(0.987 0.022 95.277)" },
          "100": { value: "oklch(0.962 0.059 95.617)" },
          "200": { value: "oklch(0.924 0.12 95.746)" },
          "300": { value: "oklch(0.879 0.169 91.605)" },
          "400": { value: "oklch(0.828 0.189 84.429)" },
          "500": { value: "oklch(0.769 0.188 70.08)" },
          "600": { value: "oklch(0.666 0.179 58.318)" },
          "700": { value: "oklch(0.555 0.163 48.998)" },
          "800": { value: "oklch(0.473 0.137 46.201)" },
          "900": { value: "oklch(0.414 0.112 45.904)" },
          "950": { value: "oklch(0.279 0.077 45.635)" },
        },
        yellow: {
          "50": { value: "oklch(0.987 0.026 102.212)" },
          "100": { value: "oklch(0.973 0.071 103.193)" },
          "200": { value: "oklch(0.945 0.129 101.54)" },
          "300": { value: "oklch(0.905 0.182 98.111)" },
          "400": { value: "oklch(0.852 0.199 91.936)" },
          "500": { value: "oklch(0.795 0.184 86.047)" },
          "600": { value: "oklch(0.681 0.162 75.834)" },
          "700": { value: "oklch(0.554 0.135 66.442)" },
          "800": { value: "oklch(0.476 0.114 61.907)" },
          "900": { value: "oklch(0.421 0.095 57.708)" },
          "950": { value: "oklch(0.286 0.066 53.813)" },
        },
        lime: {
          "50": { value: "oklch(0.986 0.031 120.757)" },
          "100": { value: "oklch(0.967 0.067 122.328)" },
          "200": { value: "oklch(0.938 0.127 124.321)" },
          "300": { value: "oklch(0.897 0.196 126.665)" },
          "400": { value: "oklch(0.841 0.238 128.85)" },
          "500": { value: "oklch(0.768 0.233 130.85)" },
          "600": { value: "oklch(0.648 0.2 131.684)" },
          "700": { value: "oklch(0.532 0.157 131.589)" },
          "800": { value: "oklch(0.453 0.124 130.933)" },
          "900": { value: "oklch(0.405 0.101 131.063)" },
          "950": { value: "oklch(0.274 0.072 132.109)" },
        },
        green: {
          // Values modified from original Tailwind to improve contrast in Chakra UI
          "50": { value: "oklch(0.982 0.018 155.826)" },
          "100": { value: "oklch(0.962 0.044 156.743)" },
          "200": { value: "oklch(0.925 0.084 155.995)" },
          "300": { value: "oklch(0.75 0.18 153.0)" }, // Original: oklch(0.871 0.15 154.449)
          "400": { value: "oklch(0.625 0.209 150.0)" }, // Original: oklch(0.792 0.209 151.711)
          "500": { value: "oklch(0.528 0.219 149.579)" }, // Original: oklch(0.723 0.219 149.579)
          "600": { value: "oklch(0.47 0.20 149.0)" }, // Original: oklch(0.627 0.194 149.214)
          "700": { value: "oklch(0.40 0.16 149.5)" }, // Original: oklch(0.527 0.154 150.069)
          "800": { value: "oklch(0.448 0.119 151.328)" },
          "900": { value: "oklch(0.393 0.095 152.535)" },
          "950": { value: "oklch(0.266 0.065 152.934)" },
        },
        emerald: {
          "50": { value: "oklch(0.979 0.021 166.113)" },
          "100": { value: "oklch(0.95 0.052 163.051)" },
          "200": { value: "oklch(0.905 0.093 164.15)" },
          "300": { value: "oklch(0.845 0.143 164.978)" },
          "400": { value: "oklch(0.765 0.177 163.223)" },
          "500": { value: "oklch(0.696 0.17 162.48)" },
          "600": { value: "oklch(0.596 0.145 163.225)" },
          "700": { value: "oklch(0.508 0.118 165.612)" },
          "800": { value: "oklch(0.432 0.095 166.913)" },
          "900": { value: "oklch(0.378 0.077 168.94)" },
          "950": { value: "oklch(0.262 0.051 172.552)" },
        },
        teal: {
          "50": { value: "oklch(0.984 0.014 180.72)" },
          "100": { value: "oklch(0.953 0.051 180.801)" },
          "200": { value: "oklch(0.91 0.096 180.426)" },
          "300": { value: "oklch(0.855 0.138 181.071)" },
          "400": { value: "oklch(0.777 0.152 181.912)" },
          "500": { value: "oklch(0.704 0.14 182.503)" },
          "600": { value: "oklch(0.6 0.118 184.704)" },
          "700": { value: "oklch(0.511 0.096 186.391)" },
          "800": { value: "oklch(0.437 0.078 188.216)" },
          "900": { value: "oklch(0.386 0.063 188.416)" },
          "950": { value: "oklch(0.277 0.046 192.524)" },
        },
        cyan: {
          "50": { value: "oklch(0.984 0.019 200.873)" },
          "100": { value: "oklch(0.956 0.045 203.388)" },
          "200": { value: "oklch(0.917 0.08 205.041)" },
          "300": { value: "oklch(0.865 0.127 207.078)" },
          "400": { value: "oklch(0.789 0.154 211.53)" },
          "500": { value: "oklch(0.715 0.143 215.221)" },
          "600": { value: "oklch(0.609 0.126 221.723)" },
          "700": { value: "oklch(0.52 0.105 223.128)" },
          "800": { value: "oklch(0.45 0.085 224.283)" },
          "900": { value: "oklch(0.398 0.07 227.392)" },
          "950": { value: "oklch(0.302 0.056 229.695)" },
        },
        sky: {
          "50": { value: "oklch(0.977 0.013 236.62)" },
          "100": { value: "oklch(0.951 0.026 236.824)" },
          "200": { value: "oklch(0.901 0.058 230.902)" },
          "300": { value: "oklch(0.828 0.111 230.318)" },
          "400": { value: "oklch(0.746 0.16 232.661)" },
          "500": { value: "oklch(0.685 0.169 237.323)" },
          "600": { value: "oklch(0.588 0.158 241.966)" },
          "700": { value: "oklch(0.5 0.134 242.749)" },
          "800": { value: "oklch(0.443 0.11 240.79)" },
          "900": { value: "oklch(0.391 0.09 240.876)" },
          "950": { value: "oklch(0.293 0.066 243.157)" },
        },
        blue: {
          "50": { value: "oklch(0.97 0.014 254.604)" },
          "100": { value: "oklch(0.932 0.032 255.585)" },
          "200": { value: "oklch(0.882 0.059 254.128)" },
          "300": { value: "oklch(0.809 0.105 251.813)" },
          "400": { value: "oklch(0.707 0.165 254.624)" },
          "500": { value: "oklch(0.623 0.214 259.815)" },
          "600": { value: "oklch(0.546 0.245 262.881)" },
          "700": { value: "oklch(0.488 0.243 264.376)" },
          "800": { value: "oklch(0.424 0.199 265.638)" },
          "900": { value: "oklch(0.379 0.146 265.522)" },
          "950": { value: "oklch(0.282 0.091 267.935)" },
        },
        indigo: {
          "50": { value: "oklch(0.962 0.018 272.314)" },
          "100": { value: "oklch(0.93 0.034 272.788)" },
          "200": { value: "oklch(0.87 0.065 274.039)" },
          "300": { value: "oklch(0.785 0.115 274.713)" },
          "400": { value: "oklch(0.673 0.182 276.935)" },
          "500": { value: "oklch(0.585 0.233 277.117)" },
          "600": { value: "oklch(0.511 0.262 276.966)" },
          "700": { value: "oklch(0.457 0.24 277.023)" },
          "800": { value: "oklch(0.398 0.195 277.366)" },
          "900": { value: "oklch(0.359 0.144 278.697)" },
          "950": { value: "oklch(0.257 0.09 281.288)" },
        },
        violet: {
          "50": { value: "oklch(0.969 0.016 293.756)" },
          "100": { value: "oklch(0.943 0.029 294.588)" },
          "200": { value: "oklch(0.894 0.057 293.283)" },
          "300": { value: "oklch(0.811 0.111 293.571)" },
          "400": { value: "oklch(0.702 0.183 293.541)" },
          "500": { value: "oklch(0.606 0.25 292.717)" },
          "600": { value: "oklch(0.541 0.281 293.009)" },
          "700": { value: "oklch(0.491 0.27 292.581)" },
          "800": { value: "oklch(0.432 0.232 292.759)" },
          "900": { value: "oklch(0.38 0.189 293.745)" },
          "950": { value: "oklch(0.283 0.141 291.089)" },
        },
        purple: {
          "50": { value: "oklch(0.977 0.014 308.299)" },
          "100": { value: "oklch(0.946 0.033 307.174)" },
          "200": { value: "oklch(0.902 0.063 306.703)" },
          "300": { value: "oklch(0.827 0.119 306.383)" },
          "400": { value: "oklch(0.714 0.203 305.504)" },
          "500": { value: "oklch(0.627 0.265 303.9)" },
          "600": { value: "oklch(0.558 0.288 302.321)" },
          "700": { value: "oklch(0.496 0.265 301.924)" },
          "800": { value: "oklch(0.438 0.218 303.724)" },
          "900": { value: "oklch(0.381 0.176 304.987)" },
          "950": { value: "oklch(0.291 0.149 302.717)" },
        },
        fuchsia: {
          "50": { value: "oklch(0.977 0.017 320.058)" },
          "100": { value: "oklch(0.952 0.037 318.852)" },
          "200": { value: "oklch(0.903 0.076 319.62)" },
          "300": { value: "oklch(0.833 0.145 321.434)" },
          "400": { value: "oklch(0.74 0.238 322.16)" },
          "500": { value: "oklch(0.667 0.295 322.15)" },
          "600": { value: "oklch(0.591 0.293 322.896)" },
          "700": { value: "oklch(0.518 0.253 323.949)" },
          "800": { value: "oklch(0.452 0.211 324.591)" },
          "900": { value: "oklch(0.401 0.17 325.612)" },
          "950": { value: "oklch(0.293 0.136 325.661)" },
        },
        pink: {
          "50": { value: "oklch(0.971 0.014 343.198)" },
          "100": { value: "oklch(0.948 0.028 342.258)" },
          "200": { value: "oklch(0.899 0.061 343.231)" },
          "300": { value: "oklch(0.823 0.12 346.018)" },
          "400": { value: "oklch(0.718 0.202 349.761)" },
          "500": { value: "oklch(0.656 0.241 354.308)" },
          "600": { value: "oklch(0.592 0.249 0.584)" },
          "700": { value: "oklch(0.525 0.223 3.958)" },
          "800": { value: "oklch(0.459 0.187 3.815)" },
          "900": { value: "oklch(0.408 0.153 2.432)" },
          "950": { value: "oklch(0.284 0.109 3.907)" },
        },
        rose: {
          "50": { value: "oklch(0.969 0.015 12.422)" },
          "100": { value: "oklch(0.941 0.03 12.58)" },
          "200": { value: "oklch(0.892 0.058 10.001)" },
          "300": { value: "oklch(0.81 0.117 11.638)" },
          "400": { value: "oklch(0.712 0.194 13.428)" },
          "500": { value: "oklch(0.645 0.246 16.439)" },
          "600": { value: "oklch(0.586 0.253 17.585)" },
          "700": { value: "oklch(0.514 0.222 16.935)" },
          "800": { value: "oklch(0.455 0.188 13.697)" },
          "900": { value: "oklch(0.41 0.159 10.272)" },
          "950": { value: "oklch(0.271 0.105 12.094)" },
        },
        slate: {
          "50": { value: "oklch(0.984 0.003 247.858)" },
          "100": { value: "oklch(0.968 0.007 247.896)" },
          "200": { value: "oklch(0.929 0.013 255.508)" },
          "300": { value: "oklch(0.869 0.022 252.894)" },
          "400": { value: "oklch(0.704 0.04 256.788)" },
          "500": { value: "oklch(0.554 0.046 257.417)" },
          "600": { value: "oklch(0.446 0.043 257.281)" },
          "700": { value: "oklch(0.372 0.044 257.287)" },
          "800": { value: "oklch(0.279 0.041 260.031)" },
          "900": { value: "oklch(0.208 0.042 265.755)" },
          "950": { value: "oklch(0.129 0.042 264.695)" },
        },
        zinc: {
          "50": { value: "oklch(0.985 0 0)" },
          "100": { value: "oklch(0.967 0.001 286.375)" },
          "200": { value: "oklch(0.92 0.004 286.32)" },
          "300": { value: "oklch(0.871 0.006 286.286)" },
          "400": { value: "oklch(0.705 0.015 286.067)" },
          "500": { value: "oklch(0.552 0.016 285.938)" },
          "600": { value: "oklch(0.442 0.017 285.786)" },
          "700": { value: "oklch(0.37 0.013 285.805)" },
          "800": { value: "oklch(0.274 0.006 286.033)" },
          "900": { value: "oklch(0.21 0.006 285.885)" },
          "950": { value: "oklch(0.141 0.005 285.823)" },
        },
        neutral: {
          "50": { value: "oklch(0.985 0 0)" },
          "100": { value: "oklch(0.97 0 0)" },
          "200": { value: "oklch(0.922 0 0)" },
          "300": { value: "oklch(0.87 0 0)" },
          "400": { value: "oklch(0.708 0 0)" },
          "500": { value: "oklch(0.556 0 0)" },
          "600": { value: "oklch(0.439 0 0)" },
          "700": { value: "oklch(0.371 0 0)" },
          "800": { value: "oklch(0.269 0 0)" },
          "900": { value: "oklch(0.205 0 0)" },
          "950": { value: "oklch(0.145 0 0)" },
        },
        stone: {
          "50": { value: "oklch(0.985 0.001 106.423)" },
          "100": { value: "oklch(0.97 0.001 106.424)" },
          "200": { value: "oklch(0.923 0.003 48.717)" },
          "300": { value: "oklch(0.869 0.005 56.366)" },
          "400": { value: "oklch(0.709 0.01 56.259)" },
          "500": { value: "oklch(0.553 0.013 58.071)" },
          "600": { value: "oklch(0.444 0.011 73.639)" },
          "700": { value: "oklch(0.374 0.01 67.558)" },
          "800": { value: "oklch(0.268 0.007 34.298)" },
          "900": { value: "oklch(0.216 0.006 56.043)" },
          "950": { value: "oklch(0.147 0.004 49.25)" },
        },
      },
    },
    semanticTokens: {
      colors: {
        // Brand colors for consistent theming
        brand: generateSemanticTokens("brand"),
        // GENERIC STATE
        danger: generateSemanticTokens("red"),
        info: generateSemanticTokens("blue"),
        warning: generateSemanticTokens("amber"),
        error: generateSemanticTokens("red"),
        // TAILWIND 4.0 COLORS
        red: generateSemanticTokens("red"),
        orange: generateSemanticTokens("orange"),
        amber: generateSemanticTokens("amber"),
        yellow: generateSemanticTokens("yellow"),
        lime: generateSemanticTokens("lime"),
        green: generateSemanticTokens("green"),
        emerald: generateSemanticTokens("emerald"),
        teal: generateSemanticTokens("teal"),
        cyan: generateSemanticTokens("cyan"),
        sky: generateSemanticTokens("sky"),
        blue: generateSemanticTokens("blue"),
        indigo: generateSemanticTokens("indigo"),
        violet: generateSemanticTokens("violet"),
        purple: generateSemanticTokens("purple"),
        fuchsia: generateSemanticTokens("fuchsia"),
        pink: generateSemanticTokens("pink"),
        rose: generateSemanticTokens("rose"),
        slate: generateSemanticTokens("slate"),
        gray: generateSemanticTokens("gray"),
        zinc: generateSemanticTokens("zinc"),
        neutral: generateSemanticTokens("neutral"),
        stone: generateSemanticTokens("stone"),
        // Task state components - unified colors across all uses
        taskState: {
          active: generateSemanticTokens("blue"),
          success: generateSemanticTokens("green"),
          failed: generateSemanticTokens("red"),
          queued: generateSemanticTokens("stone"),
          skipped: generateSemanticTokens("pink"),
          up_for_reschedule: generateSemanticTokens("sky"),
          up_for_retry: generateSemanticTokens("yellow"),
          upstream_failed: generateSemanticTokens("orange"),
          running: generateSemanticTokens("cyan"),
          restarting: generateSemanticTokens("violet"),
          deferred: generateSemanticTokens("purple"),
          scheduled: generateSemanticTokens("zinc"),
          none: generateSemanticTokens("gray"),
          removed: generateSemanticTokens("slate"),
        },
        // Pool and slot management
        pool: {
          bar: {
            background: { value: "{colors.bg.subtle}" },
            border: { value: "{colors.gray.muted}" },
            openSlots: { value: "{colors.success.solid}" },
            runningSlots: { value: "{colors.running.solid}" },
            queuedSlots: { value: "{colors.queued.solid}" },
            scheduledSlots: { value: "{colors.scheduled.solid}" },
            deferredSlots: { value: "{colors.deferred.solid}" },
          },
          card: {
            background: { value: "{colors.bg.subtle}" },
            border: { value: "{colors.gray.muted}" },
            header: { value: "{colors.brand.solid}" },
          },
          form: {
            background: { value: "{colors.bg.subtle}" },
            border: { value: "{colors.gray.muted}" },
            input: { value: "{colors.gray.solid}" },
          },
        },
        // Chart and visualization tokens
        chart: {
          duration: {
            background: { value: "{colors.bg}" },
            grid: { value: "{colors.gray.muted}" },
            axis: { value: "{colors.gray.solid}" },
            bar: { value: "{colors.brand.solid}" },
            barHover: { value: "{colors.brand.emphasized}" },
            selected: { value: "{colors.brand.fg}" },
          },
          gantt: {
            background: { value: "{colors.bg}" },
            grid: { value: "{colors.gray.muted}" },
            rowHeight: { value: "20px" },
            barHeight: { value: "16px" },
            selectedItem: { value: "{colors.brand.solid}" },
            taskBar: { value: "{colors.brand.solid}" },
            taskBarHover: { value: "{colors.brand.emphasized}" },
          },
          trend: {
            background: { value: "{colors.bg}" },
            line: { value: "{colors.brand.solid}" },
            point: { value: "{colors.brand.solid}" },
            pointHover: { value: "{colors.brand.emphasized}" },
            grid: { value: "{colors.gray.muted}" },
          },
        },
        // Calendar visualization tokens
        calendar: {
          empty: { value: { _dark: "transparent", _light: "transparent" } },
          totalRuns: {
            level1: { value: { _dark: "{colors.brand.300}", _light: "{colors.brand.200}" } }, // 1-5 runs
            level2: { value: { _dark: "{colors.brand.500}", _light: "{colors.brand.400}" } }, // 6-15 runs
            level3: { value: { _dark: "{colors.brand.700}", _light: "{colors.brand.600}" } }, // 16-25 runs
            level4: { value: { _dark: "{colors.brand.900}", _light: "{colors.brand.800}" } }, // 26+ runs
          },
          failedRuns: {
            level1: { value: { _dark: "{colors.red.300}", _light: "{colors.red.200}" } }, // 1-2 failures
            level2: { value: { _dark: "{colors.red.500}", _light: "{colors.red.400}" } }, // 3-5 failures
            level3: { value: { _dark: "{colors.red.700}", _light: "{colors.red.600}" } }, // 6-10 failures
            level4: { value: { _dark: "{colors.red.900}", _light: "{colors.red.800}" } }, // 11+ failures
          },
          planned: { value: { _dark: "{colors.scheduled.600}", _light: "{colors.scheduled.200}" } },
          loading: {
            overlay: { value: "{colors.bg.subtle}" },
            spinner: {
              border: { value: "{colors.border.emphasized}" },
              borderTop: { value: "{colors.brand.solid}" },
            },
          },
        },
        // General loading and overlay tokens
        loading: {
          overlay: { value: "{colors.bg.subtle}" },
          spinner: {
            border: { value: "{colors.border.emphasized}" },
            borderTop: { value: "{colors.brand.solid}" },
          },
        },
        // Human-in-the-loop (HITL) tokens
        hitl: {
          background: { value: "{colors.warning.subtle}" },
          border: { value: "{colors.warning.solid}" },
          text: { value: "{colors.warning.fg}" },
          button: { value: "{colors.warning.solid}" },
          buttonHover: { value: "{colors.warning.emphasized}" },
          form: { value: "{colors.bg.subtle}" },
          response: { value: "{colors.success.solid}" },
        },
        // Application semantic tokens
        app: {
          // Layout components
          baseLayout: {
            background: { value: "{colors.bg}" },
            content: { value: "{colors.bg}" },
            sidebar: { value: "{colors.brand.muted}" },
          },
          nav: {
            background: { value: "{colors.brand.muted}" },
            button: { value: "{colors.brand.solid}" },
            buttonHover: { value: "{colors.brand.emphasized}" },
            buttonActive: { value: "{colors.brand.fg}" },
          },
          detailsLayout: {
            background: { value: "{colors.bg}" },
            panel: { value: "{colors.bg.subtle}" },
            breadcrumb: { value: "{colors.gray.muted}" },
            tabs: { value: "{colors.gray.solid}" },
            toggleButton: { value: "{colors.fg.subtle}" },
            resizeHandle: { value: "{colors.border.emphasized}" },
          },
          // Page components
          dashboard: {
            background: { value: "{colors.bg}" },
            card: { value: "{colors.bg.subtle}" },
            metric: { value: "{colors.brand.solid}" },
            health: { value: "{colors.success.solid}" },
            warning: { value: "{colors.warning.solid}" },
            error: { value: "{colors.error.solid}" },
          },
          dagsList: {
            background: { value: "{colors.bg}" },
            card: { value: "{colors.bg.subtle}" },
            cardHover: { value: "{colors.bg.emphasized}" },
            filter: { value: "{colors.gray.muted}" },
            sort: { value: "{colors.gray.solid}" },
          },
          dagDetails: {
            background: { value: "{colors.bg}" },
            header: { value: "{colors.bg.subtle}" },
            overview: { value: "{colors.bg.subtle}" },
            tasks: { value: "{colors.bg.subtle}" },
            calendar: { value: "{colors.bg.subtle}" },
            code: { value: "{colors.bg.subtle}" },
            backfills: { value: "{colors.bg.subtle}" },
          },
          dagGraph: {
            reactflow: {
              background: {
                value: {
                  _light: "{colors.brand.50}",
                  _dark: "{colors.brand.950}"
                }
              },
              backgroundPattern: {
                value: {
                  _light: "{colors.gray.800}",
                  _dark: "{colors.gray.200}"
                }
              },
              controlsButton: {
                value: {
                  _light: "white",
                  _dark: "{colors.gray.800}"
                }
              },
              controlsButtonHover: {
                value: {
                  _light: "{colors.gray.100}",
                  _dark: "{colors.gray.700}"
                }
              },
              minimapBackground: {
                value: "{colors.bg}"
              },
            },
          },
          tasksList: {
            background: { value: "{colors.bg}" },
            card: { value: "{colors.bg.subtle}" },
            filter: { value: "{colors.gray.muted}" },
          },
          taskDetails: {
            background: { value: "{colors.bg}" },
            header: { value: "{colors.bg.subtle}" },
            overview: { value: "{colors.bg.subtle}" },
            logs: { value: "{colors.bg.subtle}" },
          },
          taskInstance: {
            background: { value: "{colors.bg}" },
            header: { value: "{colors.bg.subtle}" },
            details: { value: "{colors.bg.subtle}" },
            logs: { value: "{colors.bg.subtle}" },
          },
          assetsList: {
            background: { value: "{colors.bg}" },
            card: { value: "{colors.bg.subtle}" },
            dependency: { value: "{colors.brand.solid}" },
          },
          assetDetails: {
            background: { value: "{colors.bg}" },
            header: { value: "{colors.bg.subtle}" },
            graph: { value: "{colors.bg.subtle}" },
            events: { value: "{colors.bg.subtle}" },
          },
          connections: {
            background: { value: "{colors.bg}" },
            form: { value: "{colors.bg.subtle}" },
            button: { value: "{colors.brand.solid}" },
            buttonHover: { value: "{colors.brand.emphasized}" },
          },
          variables: {
            background: { value: "{colors.bg}" },
            form: { value: "{colors.bg.subtle}" },
            button: { value: "{colors.brand.solid}" },
            buttonHover: { value: "{colors.brand.emphasized}" },
          },
          pools: {
            background: { value: "{colors.bg}" },
            form: { value: "{colors.bg.subtle}" },
            button: { value: "{colors.brand.solid}" },
            buttonHover: { value: "{colors.brand.emphasized}" },
          },
          events: {
            background: { value: "{colors.bg}" },
            filter: { value: "{colors.gray.muted}" },
          },
          xcom: {
            background: { value: "{colors.bg}" },
            entry: { value: "{colors.bg.subtle}" },
            filter: { value: "{colors.gray.muted}" },
          },
          configs: {
            background: { value: "{colors.bg}" },
            form: { value: "{colors.bg.subtle}" },
          },
          providers: {
            background: { value: "{colors.bg}" },
          },
          plugins: {
            background: { value: "{colors.bg}" },
          },
          security: {
            background: { value: "{colors.bg}" },
          },
          // Shared components
          dataDisplay: {
            breadcrumbStats: { value: "{colors.gray.solid}" },
            dagRunInfo: { value: "{colors.brand.solid}" },
            dagVersion: { value: "{colors.gray.solid}" },
            durationChart: { value: "{colors.brand.solid}" },
            headerCard: { value: "{colors.bg.subtle}" },
            limitedItemsList: { value: "{colors.gray.solid}" },
            needsReviewBadge: { value: "{colors.warning.solid}" },
            needsReviewButton: { value: "{colors.warning.solid}" },
            poolBar: { value: "{colors.brand.solid}" },
            renderedJsonField: { value: "{colors.gray.solid}" },
            runTypeIcon: { value: "{colors.brand.solid}" },
            stateBadge: { value: "{colors.success.solid}" },
            stateIcon: { value: "{colors.success.solid}" },
            statsCard: { value: "{colors.bg.subtle}" },
            taskInstanceTooltip: { value: "{colors.gray.solid}" },
            taskName: { value: "{colors.brand.solid}" },
            taskTrySelect: { value: "{colors.gray.solid}" },
            time: { value: "{colors.gray.solid}" },
            timeRangeSelector: { value: "{colors.gray.solid}" },
            trendCountButton: { value: "{colors.brand.solid}" },
            trendCountChart: { value: "{colors.brand.solid}" },
            truncatedText: { value: "{colors.gray.solid}" },
          },
          forms: {
            configForm: { value: "{colors.bg.subtle}" },
            dateTimeInput: { value: "{colors.gray.solid}" },
            editableMarkdownArea: { value: "{colors.bg.subtle}" },
            editableMarkdownButton: { value: "{colors.brand.solid}" },
            jsonEditor: { value: "{colors.bg.subtle}" },
            searchBar: { value: "{colors.gray.muted}" },
            searchDags: { value: "{colors.brand.solid}" },
            searchDagsButton: { value: "{colors.brand.solid}" },
            searchDagsDropdown: { value: "{colors.gray.solid}" },
          },
          modals: {
            confirmationModal: { value: "{colors.bg.subtle}" },
            deleteDialog: { value: "{colors.error.solid}" },
            displayMarkdownButton: { value: "{colors.brand.solid}" },
            displayMarkdownHeader: { value: "{colors.info.muted}" },
          },
          alerts: {
            errorAlert: { value: "{colors.error.solid}" },
            warningAlert: { value: "{colors.warning.solid}" },
          },
          actions: {
            clear: {
              run: {
                button: { value: "{colors.error.solid}" },
                dialog: { value: "{colors.error.solid}" },
              },
              taskInstance: {
                button: { value: "{colors.error.solid}" },
                dialog: { value: "{colors.error.solid}" },
                groupDialog: { value: "{colors.error.solid}" },
              },
            },
            dagActions: {
              deleteDagButton: { value: "{colors.error.solid}" },
              favoriteDagButton: { value: "{colors.brand.solid}" },
              inlineMessage: { value: "{colors.gray.solid}" },
              parseDag: { value: "{colors.brand.solid}" },
              runBackfillForm: { value: "{colors.brand.solid}" },
            },
            markAs: {
              run: {
                button: { value: "{colors.brand.solid}" },
                dialog: { value: "{colors.brand.solid}" },
              },
              taskInstance: {
                button: { value: "{colors.brand.solid}" },
                dialog: { value: "{colors.brand.solid}" },
                groupDialog: { value: "{colors.brand.solid}" },
              },
            },
            triggerDag: {
              button: { value: "{colors.brand.solid}" },
              form: { value: "{colors.brand.solid}" },
              modal: { value: "{colors.brand.solid}" },
              editableMarkdown: { value: "{colors.brand.solid}" },
            },
            togglePause: { value: "{colors.warning.solid}" },
            quickFilterButton: { value: "{colors.gray.solid}" },
          },
          dataTable: {
            background: { value: "{colors.bg}" },
            cardList: { value: "{colors.bg.subtle}" },
            tableList: { value: "{colors.bg.subtle}" },
            filterMenuButton: { value: "{colors.gray.solid}" },
            toggleTableDisplay: { value: "{colors.gray.solid}" },
            skeleton: { value: "{colors.gray.muted}" },
          },
          flexibleForm: {
            background: { value: "{colors.bg.subtle}" },
            fieldAdvancedArray: { value: "{colors.gray.solid}" },
            fieldBool: { value: "{colors.gray.solid}" },
            fieldDateTime: { value: "{colors.gray.solid}" },
            fieldDropdown: { value: "{colors.gray.solid}" },
            fieldMultilineText: { value: "{colors.gray.solid}" },
            fieldMultiSelect: { value: "{colors.gray.solid}" },
            fieldNumber: { value: "{colors.gray.solid}" },
            fieldObject: { value: "{colors.gray.solid}" },
            fieldRow: { value: "{colors.gray.solid}" },
            fieldSelector: { value: "{colors.gray.solid}" },
            fieldString: { value: "{colors.gray.solid}" },
            fieldStringArray: { value: "{colors.gray.solid}" },
            hiddenInput: { value: "{colors.gray.solid}" },
            row: { value: "{colors.gray.solid}" },
            isParamRequired: { value: "{colors.error.solid}" },
          },
          graph: {
            aliasNode: { value: "{colors.brand.solid}" },
            assetConditionNode: { value: "{colors.brand.solid}" },
            assetNode: { value: "{colors.brand.solid}" },
            dagNode: { value: "{colors.brand.solid}" },
            defaultNode: { value: "{colors.gray.solid}" },
            downloadButton: { value: "{colors.brand.solid}" },
            edge: { value: "{colors.gray.solid}" },
            joinNode: { value: "{colors.brand.solid}" },
            nodeWrapper: { value: "{colors.bg.subtle}" },
            taskLink: { value: "{colors.brand.solid}" },
            taskNode: { value: "{colors.brand.solid}" },
          },
          asset: {
            expression: { value: "{colors.brand.solid}" },
            node: { value: "{colors.brand.solid}" },
            andGateNode: { value: "{colors.brand.solid}" },
            orGateNode: { value: "{colors.brand.solid}" },
            event: { value: "{colors.brand.solid}" },
            events: { value: "{colors.brand.solid}" },
            triggeredRuns: { value: "{colors.brand.solid}" },
          },
          banner: {
            backfillBanner: { value: "{colors.warning.solid}" },
          },
          actionAccordion: {
            background: { value: "{colors.bg.subtle}" },
            columns: { value: "{colors.gray.solid}" },
          },
          utility: {
            reactMarkdown: { value: "{colors.gray.solid}" },
            stat: { value: "{colors.brand.solid}" },
          },
          // Search and filtering components
          search: {
            searchBar: { value: "{colors.gray.muted}" },
            searchButton: { value: "{colors.brand.solid}" },
            searchDropdown: { value: "{colors.gray.solid}" },
            filterButton: { value: "{colors.gray.solid}" },
            filterMenu: { value: "{colors.bg.subtle}" },
            quickFilter: { value: "{colors.gray.solid}" },
          },
          // Navigation and breadcrumb components
          navigation: {
            breadcrumb: { value: "{colors.gray.solid}" },
            breadcrumbActive: { value: "{colors.brand.solid}" },
            breadcrumbSeparator: { value: "{colors.gray.muted}" },
            navButton: { value: "{colors.brand.solid}" },
            navButtonActive: { value: "{colors.brand.fg}" },
            navButtonHover: { value: "{colors.brand.emphasized}" },
          },
          // Time and date components
          time: {
            timeDisplay: { value: "{colors.gray.solid}" },
            timeRangeSelector: { value: "{colors.gray.solid}" },
            timezoneSelector: { value: "{colors.gray.solid}" },
            dateTimeInput: { value: "{colors.gray.solid}" },
          },
          // Log and code display components
          logs: {
            background: { value: "{colors.bg.subtle}" },
            text: { value: "{colors.gray.solid}" },
            error: { value: "{colors.error.solid}" },
            warning: { value: "{colors.warning.solid}" },
            info: { value: "{colors.info.solid}" },
            debug: { value: "{colors.gray.muted}" },
            lineNumber: { value: "{colors.gray.muted}" },
            timestamp: { value: "{colors.gray.muted}" },
            level: { value: "{colors.brand.solid}" },
          },
          // Code display components
          code: {
            background: { value: "{colors.bg.subtle}" },
            text: { value: "{colors.gray.solid}" },
            keyword: { value: "{colors.brand.solid}" },
            string: { value: "{colors.success.solid}" },
            comment: { value: "{colors.gray.muted}" },
            number: { value: "{colors.warning.solid}" },
            function: { value: "{colors.info.solid}" },
            variable: { value: "{colors.gray.solid}" },
            lineNumber: { value: "{colors.gray.muted}" },
          },
          // UI component semantic tokens
          ui: {
            chakra: {
              button: { value: "{colors.brand.solid}" },
              actionButton: { value: "{colors.brand.solid}" },
              checkbox: { value: "{colors.brand.solid}" },
              closeButton: { value: "{colors.gray.solid}" },
              inputGroup: { value: "{colors.gray.muted}" },
              numberInput: { value: "{colors.gray.solid}" },
              progressBar: { value: "{colors.brand.solid}" },
              radioCard: { value: "{colors.brand.solid}" },
              resetButton: { value: "{colors.gray.solid}" },
              segmentedControl: { value: "{colors.brand.solid}" },
              switch: { value: "{colors.brand.solid}" },
              tag: { value: "{colors.brand.solid}" },
              tooltip: { value: "{colors.gray.solid}" },
              accordion: { value: "{colors.bg.subtle}" },
              accordionItem: { value: "{colors.bg.subtle}" },
              accordionButton: { value: "{colors.brand.solid}" },
              alert: { value: "{colors.warning.solid}" },
              breadcrumb: { value: "{colors.gray.solid}" },
              clipboard: { value: "{colors.brand.solid}" },
              dialog: { value: "{colors.bg.subtle}" },
              dialogHeader: { value: "{colors.bg.subtle}" },
              dialogBody: { value: "{colors.bg.subtle}" },
              dialogFooter: { value: "{colors.bg.subtle}" },
              fileUpload: { value: "{colors.brand.solid}" },
              fileUploadButton: { value: "{colors.brand.solid}" },
              fileUploadDropzone: { value: "{colors.brand.solid}" },
              fileUploadPreview: { value: "{colors.brand.solid}" },
              fileUploadProgress: { value: "{colors.brand.solid}" },
              fileUploadError: { value: "{colors.error.solid}" },
              menu: { value: "{colors.bg.subtle}" },
              menuItem: { value: "{colors.gray.solid}" },
              pagination: { value: "{colors.brand.solid}" },
              paginationButton: { value: "{colors.brand.solid}" },
              paginationEllipsis: { value: "{colors.gray.solid}" },
              paginationFirst: { value: "{colors.brand.solid}" },
              paginationLast: { value: "{colors.brand.solid}" },
              paginationNext: { value: "{colors.brand.solid}" },
              paginationPrevious: { value: "{colors.brand.solid}" },
              paginationInfo: { value: "{colors.gray.solid}" },
              paginationSizeSelector: { value: "{colors.gray.solid}" },
              popover: { value: "{colors.bg.subtle}" },
              popoverTrigger: { value: "{colors.brand.solid}" },
              popoverContent: { value: "{colors.bg.subtle}" },
              popoverArrow: { value: "{colors.bg.subtle}" },
              select: { value: "{colors.gray.solid}" },
              selectTrigger: { value: "{colors.gray.solid}" },
              selectContent: { value: "{colors.bg.subtle}" },
              selectItem: { value: "{colors.gray.solid}" },
              selectLabel: { value: "{colors.gray.solid}" },
              selectSeparator: { value: "{colors.gray.muted}" },
              selectGroup: { value: "{colors.gray.solid}" },
              toaster: { value: "{colors.bg.subtle}" },
              toast: { value: "{colors.bg.subtle}" },
            },
            airflow: {
              actionBar: { value: "{colors.bg.subtle}" },
              actionBarItem: { value: "{colors.brand.solid}" },
              dagWarningsModal: { value: "{colors.warning.solid}" },
            },
          },
        },
      },
    },
  },
});

export const system = createSystem(defaultConfig, customConfig);

// Utility function to resolve CSS variables to their computed values
// See: https://github.com/chakra-ui/panda/discussions/2200
export const getComputedCSSVariableValue = (variable: string): string =>
  getComputedStyle(document.documentElement)
    .getPropertyValue(variable.slice(4, variable.length - 1))
    .trim();

/**
 * Returns ReactFlow style props that use Chakra UI semantic tokens
 * The semantic tokens automatically handle light/dark mode switching
 *
 * @returns Style object to pass to ReactFlow's style prop
 *
 * @example
 * ```tsx
 * import { getReactFlowThemeStyle } from "src/theme";
 *
 * <ReactFlow
 *   style={getReactFlowThemeStyle()}
 *   nodes={nodes}
 *   edges={edges}
 * />
 * ```
 */
export const getReactFlowThemeStyle = (): CSSProperties =>
  ({
    // Background
    "--xy-background-color": "var(--chakra-colors-app-dagGraph-reactflow-background)",
    "--xy-background-pattern-color": "var(--chakra-colors-app-dagGraph-reactflow-backgroundPattern)",

    // Controls
    "--xy-controls-button-background-color": "var(--chakra-colors-app-dagGraph-reactflow-controlsButton)",
    "--xy-controls-button-background-color-hover": "var(--chakra-colors-app-dagGraph-reactflow-controlsButtonHover)",

    // MiniMap
    "--xy-minimap-background-color": "var(--chakra-colors-app-dagGraph-reactflow-minimapBackground)",

  }) as CSSProperties;
