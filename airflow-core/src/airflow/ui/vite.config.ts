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
import babel from "@rolldown/plugin-babel";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import cssInjectedByJsPlugin from "vite-plugin-css-injected-by-js";
import { defineConfig } from "vitest/config";

// https://vitejs.dev/config/
export default defineConfig({
  base: "./",
  build: { chunkSizeWarningLimit: 1600, manifest: true },
  optimizeDeps: {
    exclude: ["@guanmingchiu/sqlparser-ts"], // WASM package needs to be excluded from pre-bundling
  },
  plugins: [
    react(),
    babel({
      presets: [reactCompilerPreset()],
    }),
    // Replace the directory to work with the flask plugin generation
    {
      name: "transform-url-src",
      transformIndexHtml: (html) =>
        html
          .replaceAll(`src="./assets/`, `src="./static/assets/`)
          .replaceAll(`href="./assets/`, `href="./static/assets/`)
          .replaceAll(`href="/`, `href="./`),
    },
    // Keep Monaco's codicon CSS as a real CSS file (rather than inlined into JS).
    // The codicon stylesheet references `codicon.ttf` with a CSS-relative URL — when
    // it gets inlined into a `<style>` tag the URL resolves against the page origin
    // (the api-server) instead of the asset directory and the font fails to load.
    // Keeping the CSS as an emitted file lets the browser resolve the URL relative
    // to the stylesheet's own location (`/static/assets/`). Vite still chunks it so
    // it only loads on the routes that pull Monaco in.
    cssInjectedByJsPlugin({
      cssAssetsFilterFunction: (asset: { fileName: string }) => !asset.fileName.includes("codicon"),
    }),
  ],
  resolve: { alias: { openapi: "/openapi-gen", src: "/src" } },
  server: {
    cors: true, // Only used by the dev server.
    // The dev SPA shell is served by the airflow api-server (a different origin), so
    // Vite must emit fully-qualified URLs — otherwise asset paths (notably worker
    // module URLs) resolve against the api-server origin and 404. The `dev` script
    // pins this port via --strictPort.
    origin: "http://localhost:5173",
    proxy: {
      "/hitl-review": {
        changeOrigin: true,
        target: "http://localhost:28080",
      },
    },
  },
  test: {
    coverage: {
      include: ["src/**/*.ts", "src/**/*.tsx"],
    },
    css: true,
    environment: "happy-dom",
    exclude: ["**/node_modules/**", "**/dist/**", "tests/e2e/**"],
    globals: true,
    mockReset: true,
    restoreMocks: true,
    setupFiles: "./testsSetup.ts",
  },
});
