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

// Strip the ASF license header from the landing page before TypeDoc reads it.
//
// ``index.md`` carries the standard ``<!-- ... -->`` ASF header that the
// insert-license hook enforces on every Markdown file. clean-jsdoc-theme renders
// page bodies through MDX, where ``<!--`` is a syntax error (MDX comments are
// ``{/* */}``), so the header has to go before TypeDoc picks the file up as its
// ``readme``. Writing the stripped copy to a gitignored scratch file keeps the
// committed source compliant and the rendered page clean.

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const docsRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const source = resolve(docsRoot, "index.md");
const target = resolve(docsRoot, ".typedoc", "readme.md");

const LEADING_HTML_COMMENT = /^\s*<!--[\s\S]*?-->\s*/;

const markdown = await readFile(source, "utf8");
if (!LEADING_HTML_COMMENT.test(markdown)) {
  throw new Error(`Expected an ASF license header at the top of ${source}`);
}

await mkdir(dirname(target), { recursive: true });
await writeFile(target, markdown.replace(LEADING_HTML_COMMENT, ""));
