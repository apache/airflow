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

// Remove the theme's Google Fonts tags; ASF privacy guidance allows them only
// when ASF-hosted. There is no theme option: it validates the font families
// against the live Google Fonts API and restores its own defaults on a 400, so
// overriding them regresses in CI, where the build has network. The theme's CSS
// already declares Georgia / system-ui fallbacks, so nothing is substituted.

import { readFile, writeFile } from "node:fs/promises";

import { HTML_ROOT, filesUnder, stripRemoteFontTags } from "./deployment-checks.mjs";

let scanned = 0;
let stripped = 0;

for await (const file of filesUnder(HTML_ROOT, [".html"])) {
  scanned += 1;
  const html = await readFile(file, "utf8");
  const cleaned = stripRemoteFontTags(html);
  if (cleaned !== html) {
    await writeFile(file, cleaned, "utf8");
    stripped += 1;
  }
}

console.log(`strip-remote-fonts: removed remote font tags from ${stripped}/${scanned} page(s).`);
