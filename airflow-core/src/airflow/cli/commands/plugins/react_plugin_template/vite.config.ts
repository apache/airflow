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
import { defineConfig } from "vitest/config";

// https://vitejs.dev/config/
export default defineConfig(({ command }) => {
  const isLibraryBuild = command === 'build';

  return {
    base: "./",
    build: isLibraryBuild ? {
      chunkSizeWarningLimit: 1600,
      lib: {
        entry: resolve("src", "main.tsx"),
        fileName: 'main',
        formats: ['es'],
        name: '{{PROJECT_NAME}}'
      },
      rollupOptions: {
        external: [
          'react',
          'react-dom',
          '@chakra-ui/react',
          '@emotion/react',
          '@tanstack/react-query',
          '@tanstack/react-table',
          '@tanstack/react-virtual',
          'axios',
          'chakra-react-select',
          'dayjs',
          'i18next',
          'i18next-browser-languagedetector',
          'i18next-http-backend',
          'next-themes',
          'react-hook-form',
          'react-i18next',
          'react-icons',
          'react-router-dom',
          'use-debounce',
          'usehooks-ts',
          'zustand'
        ],
        output: {
          globals: {
            'react': 'React',
            'react-dom': 'ReactDOM'
          }
        }
      },
      sourcemap: true
    } : {
      // Development build configuration
      chunkSizeWarningLimit: 1600
    },
    plugins: [
      react(),
      cssInjectedByJsPlugin(),
      ...(isLibraryBuild ? [dts({
        include: ["src/main.tsx"],
        insertTypesEntry: true,
        outDir: "dist"
      })] : [])
    ],
    resolve: { alias: { src: "/src" } },
    server: {
      cors: true, // Only used by the dev server.
    },
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
  };
});
