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

// Fail the build on HTML that renders locally but breaks once published.

import { readFile } from "node:fs/promises";

import {
  HTML_ROOT,
  SDK_PACKAGE_JSON,
  collectErrors,
  filesUnder,
  hasRemoteFonts,
  inspectPage,
  resolveBasePath,
} from "./deployment-checks.mjs";

// The theme can inline a font host into a stylesheet or an island bundle as well as into the
// page head, and those the stripper cannot repair -- so scan them too rather than promise a
// font-free build that only holds for HTML.
const ASSET_EXTENSIONS = [".css", ".js"];

const { version } = JSON.parse(await readFile(SDK_PACKAGE_JSON, "utf8"));
const basePath = resolveBasePath(version);

const rootRelative = [];
const remoteFonts = [];
let scanned = 0;
let usingBasePath = 0;

for await (const file of filesUnder(HTML_ROOT, [".html", ...ASSET_EXTENSIONS])) {
  const name = file.slice(HTML_ROOT.length + 1);
  const text = await readFile(file, "utf8");
  if (!file.endsWith(".html")) {
    if (hasRemoteFonts(text)) remoteFonts.push(name);
    continue;
  }
  scanned += 1;
  const result = inspectPage(text, basePath);
  if (result.rootRelative) rootRelative.push(name);
  if (result.remoteFonts) remoteFonts.push(name);
  if (result.usesBasePath) usingBasePath += 1;
}

const errors = collectErrors({ basePath, scanned, rootRelative, remoteFonts, usingBasePath });

if (errors.length > 0) {
  console.error("verify-deployment-paths: FAILED");
  for (const error of errors) console.error(`  - ${error}`);
  process.exitCode = 1;
} else {
  console.log(
    `verify-deployment-paths: ${scanned} page(s) OK (basePath "${basePath || "/"}", no remote fonts).`,
  );
}
