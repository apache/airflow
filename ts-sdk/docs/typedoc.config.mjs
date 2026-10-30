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

// clean-jsdoc-theme defaults basePath to "/", which emits every asset and link
// root-relative and leaves the published site with no CSS.
//
// The prefix is the versioned one, not /docs/ts-sdk/stable: publish-docs-to-s3
// syncs one build to both, so only one can own the assets. Pinning to the
// version keeps archived releases self-contained, since a later release
// replaces stable/ (sync --delete) without touching assets they still cite.

import { readFileSync } from "node:fs";

import { resolveBasePath, SDK_PACKAGE_JSON } from "./scripts/deployment-checks.mjs";

const { version } = JSON.parse(readFileSync(SDK_PACKAGE_JSON, "utf8"));

export default {
  plugin: ["@clean-jsdoc-theme/typedoc"],
  outputs: [
    {
      name: "clean-jsdoc-theme",
      path: "_build/html",
    },
  ],
  entryPoints: ["../api-docs/*.ts"],
  tsconfig: "./tsconfig.json",
  name: "Apache Airflow TypeScript SDK",
  readme: ".typedoc/readme.md",
  excludeInternal: true,
  excludePrivate: true,
  excludeExternals: true,
  cleanJsdocTheme: {
    siteName: "Apache Airflow TypeScript SDK",
    menu: [
      {
        title: "Airflow Docs",
        link: "https://airflow.apache.org/docs/",
        target: "_blank",
      },
      {
        title: "GitHub",
        link: "https://github.com/apache/airflow/tree/main/ts-sdk",
        target: "_blank",
      },
    ],
    footer:
      "Apache Airflow, Apache, Airflow, the Apache feather logo, and the Apache Airflow logo are either registered trademarks or trademarks of The Apache Software Foundation.",
    basePath: resolveBasePath(version),
  },
};
