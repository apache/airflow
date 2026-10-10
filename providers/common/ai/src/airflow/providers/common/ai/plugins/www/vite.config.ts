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
import react from "@vitejs/plugin-react-swc";
import { resolve } from "node:path";
import cssInjectedByJsPlugin from "vite-plugin-css-injected-by-js";
import dts from "vite-plugin-dts";
import { defineConfig } from "vite";

export default defineConfig(({ command, mode }) => {
  const isLibraryBuild = command === "build";
  // Vite's lib mode cannot bundle multiple entries into one UMD/IIFE output (each needs its
  // own global name), so each plugin bundle is built via a separate `vite build --mode <name>`
  // invocation into the same dist/ directory -- see package.json's "build" script.
  const isModelEntry = mode === "model";
  const entryFile = isModelEntry ? "model.tsx" : "main.tsx";
  const entryName = isModelEntry ? "model" : "main";

  return {
    base: "./",
    build: isLibraryBuild
      ? {
          chunkSizeWarningLimit: 1600,
          // Only the first build of a `pnpm build` run should clear stale dist/ output.
          emptyOutDir: !isModelEntry,
          lib: {
            entry: resolve("src", entryFile),
            fileName: entryName,
            formats: ["umd"],
            // The host (ReactPlugin.tsx) clears `globalThis.AirflowPlugin` before each dynamic
            // import and recaptures it under `globalThis[reactApp.name]` right after -- every
            // plugin bundle must use this same generic UMD global name, not a bundle-specific one.
            name: "AirflowPlugin",
          },
          rollupOptions: {
            external: [
              "react",
              "react-dom",
              "react/jsx-runtime",
              "@chakra-ui/react",
              "@emotion/react",
            ],
            output: {
              entryFileNames: "[name].umd.cjs",
              globals: {
                react: "React",
                "react-dom": "ReactDOM",
                "react/jsx-runtime": "ReactJSXRuntime",
                "@chakra-ui/react": "ChakraUI",
                "@emotion/react": "EmotionReact",
              },
            },
          },
        }
      : {
          chunkSizeWarningLimit: 1600,
        },
    define: {
      global: "globalThis",
      "process.env": "{}",
      "process.env.NODE_ENV": JSON.stringify("production"),
    },
    plugins: [
      react(),
      cssInjectedByJsPlugin(),
      ...(isLibraryBuild
        ? [
            dts({
              include: [`src/${entryFile}`],
              insertTypesEntry: true,
              outDir: "dist",
            }),
          ]
        : []),
    ],
    resolve: { alias: { src: "/src" } },
    server: {
      cors: true,
      proxy: {
        "/api": {
          changeOrigin: true,
          target: "http://localhost:28080",
        },
        "/hitl-review": {
          changeOrigin: true,
          target: "http://localhost:28080",
        },
      },
    },
  };
});
