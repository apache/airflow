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
import { createSystem, defaultConfig, defineConfig } from "@chakra-ui/react";

const generateSemanticTokens = (color: string, darkContrast: string = "white") => ({
  solid: { value: `{colors.${color}.600}` },
  contrast: { value: { _light: "white", _dark: darkContrast } },
  fg: { value: { _light: `{colors.${color}.800}`, _dark: `{colors.${color}.200}` } },
  muted: { value: { _light: `{colors.${color}.200}`, _dark: `{colors.${color}.800}` } },
  subtle: { value: { _light: `{colors.${color}.100}`, _dark: `{colors.${color}.900}` } },
  emphasized: { value: { _light: `{colors.${color}.300}`, _dark: `{colors.${color}.700}` } },
  focusRing: { value: { _light: `{colors.${color}.800}`, _dark: `{colors.${color}.200}` } },
});

const customConfig = defineConfig({
  theme: {
    tokens: {
      colors: {
        // Default green was too light
        success: {
          "50": { value: "#E0FFE0" },
          "100": { value: "#C2FFC2" },
          "200": { value: "#80FF80" },
          "300": { value: "#42FF42" },
          "400": { value: "#00FF00" },
          "500": { value: "#00C200" },
          "600": { value: "#008000" },
          "700": { value: "#006100" },
          "800": { value: "#004200" },
          "900": { value: "#001F00" },
          "950": { value: "#000F00" },
        },
        failed: defaultConfig.theme?.tokens?.colors?.red ?? {},
        // Default gray was too dark
        queued: {
          "50": { value: "#F5F5F5" },
          "100": { value: "#EBEBEB" },
          "200": { value: "#D4D4D4" },
          "300": { value: "#BFBFBF" },
          "400": { value: "#ABABAB" },
          "500": { value: "#969696" },
          "600": { value: "#808080" },
          "700": { value: "#616161" },
          "800": { value: "#404040" },
          "900": { value: "#212121" },
          "950": { value: "#0F0F0F" },
        },
        skipped: defaultConfig.theme?.tokens?.colors?.pink ?? {},
        up_for_reschedule: defaultConfig.theme?.tokens?.colors?.cyan ?? {},
        up_for_retry: defaultConfig.theme?.tokens?.colors?.yellow ?? {},
        upstream_failed: defaultConfig.theme?.tokens?.colors?.orange ?? {},
        // lime
        running: {
          "50": { value: "#EFFBEF" },
          "100": { value: "#DEF7DE" },
          "200": { value: "#B9EEB9" },
          "300": { value: "#98E698" },
          "400": { value: "#78DE78" },
          "500": { value: "#53D553" },
          "600": { value: "#32CD32" },
          "700": { value: "#269C26" },
          "800": { value: "#196719" },
          "900": { value: "#0D350D" },
          "950": { value: "#061906" },
        },
        // violet
        restarting: {
          "50": { value: "#F6EBFF" },
          "100": { value: "#EDD6FF" },
          "200": { value: "#D9A8FF" },
          "300": { value: "#C880FF" },
          "400": { value: "#B657FF" },
          "500": { value: "#A229FF" },
          "600": { value: "#8F00FF" },
          "700": { value: "#6E00C2" },
          "800": { value: "#480080" },
          "900": { value: "#260042" },
          "950": { value: "#11001F" },
        },
        // mediumpurple
        deferred: {
          "50": { value: "#F6F3FC" },
          "100": { value: "#EDE7F9" },
          "200": { value: "#DACEF3" },
          "300": { value: "#C8B6ED" },
          "400": { value: "#B9A1E7" },
          "500": { value: "#A689E1" },
          "600": { value: "#9370DB" },
          "700": { value: "#6432C8" },
          "800": { value: "#412182" },
          "900": { value: "#211041" },
          "950": { value: "#100821" },
        },
        // tan
        scheduled: {
          "50": { value: "#FBF8F4" },
          "100": { value: "#F8F3ED" },
          "200": { value: "#F1E7DA" },
          "300": { value: "#E8D9C4" },
          "400": { value: "#E1CDB2" },
          "500": { value: "#DAC1A0" },
          "600": { value: "#D2B48C" },
          "700": { value: "#B9894B" },
          "800": { value: "#7D5C31" },
          "900": { value: "#3E2E18" },
          "950": { value: "#21180D" },
        },
        // lightblue
        none: {
          "50": { value: "#F7FBFD" },
          "100": { value: "#F3F9FB" },
          "200": { value: "#E4F2F7" },
          "300": { value: "#D8ECF3" },
          "400": { value: "#C8E5EE" },
          "500": { value: "#BDDFEB" },
          "600": { value: "#ADD8E6" },
          "700": { value: "#5FB2CE" },
          "800": { value: "#30819C" },
          "900": { value: "#18414E" },
          "950": { value: "#0C2027" },
        },
        // lightgrey
        removed: {
          "50": { value: "#FCFCFC" },
          "100": { value: "#F7F7F7" },
          "200": { value: "#F0F0F0" },
          "300": { value: "#E8E8E8" },
          "400": { value: "#E0E0E0" },
          "500": { value: "#DBDBDB" },
          "600": { value: "#D3D3D3" },
          "700": { value: "#9E9E9E" },
          "800": { value: "#696969" },
          "900": { value: "#363636" },
          "950": { value: "#1A1A1A" },
        },
      },
    },
    semanticTokens: {
      colors: {
        success: generateSemanticTokens("success"),
        failed: defaultConfig.theme?.semanticTokens?.colors?.red ?? {},
        queued: generateSemanticTokens("queued"),
        skipped: defaultConfig.theme?.semanticTokens?.colors?.pink ?? {},
        up_for_reschedule: defaultConfig.theme?.semanticTokens?.colors?.cyan ?? {},
        up_for_retry: defaultConfig.theme?.semanticTokens?.colors?.yellow ?? {},
        upstream_failed: defaultConfig.theme?.semanticTokens?.colors?.orange ?? {},
        running: generateSemanticTokens("running"),
        restarting: generateSemanticTokens("restarting"),
        deferred: generateSemanticTokens("deferred"),
        scheduled: generateSemanticTokens("scheduled"),
        none: generateSemanticTokens("none", "black"),
        removed: generateSemanticTokens("removed", "black"),
      },
    },
  },
});

export const system = createSystem(defaultConfig, customConfig);
