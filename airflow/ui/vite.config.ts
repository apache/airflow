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
import { defineConfig } from "vitest/config";

// https://vitejs.dev/config/
export default defineConfig({
  build: { chunkSizeWarningLimit: 1600, manifest: true },
  plugins: [
    react(),
    // Replace the directory to work with the flask plugin generation
    {
      name: "transform-url-src",
      transformIndexHtml: (html) =>
        html
          .replace(`src="/assets/`, `src="/static/assets/`)
          .replace(`href="/`, `href="/webapp/`),
    },
  ],
  resolve: { alias: { openapi: "/openapi-gen", src: "/src" } },
  test: {
    coverage: {
      include: ["src/**/*.ts", "src/**/*.tsx"],
    },
    css: true,
    environment: "happy-dom",
    globals: true,
    mockReset: true,
    restoreMocks: true,
    setupFiles: "./testsSetup.ts",
  },
});
