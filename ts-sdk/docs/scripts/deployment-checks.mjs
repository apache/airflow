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

/** Shared helpers and paths behind the docs postbuild steps. See tests/deployment-checks.test.mjs. */

import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

// One `<link>` shape covers the preconnect hints and the stylesheet alike: the theme emits
// all three with the font host in `href`.
const REMOTE_FONT_TAG =
  /<link\b[^>]*href="https:\/\/fonts\.(?:googleapis|gstatic)\.com[^"]*"[^>]*>/g;

const ROOT_RELATIVE = /(?:href|src)="\/(?:_assets|_islands)\//;
const REMOTE_FONTS = /fonts\.(?:googleapis|gstatic)\.com/;

/** The rendered site, resolved from this file so every entry point agrees on it. */
export const HTML_ROOT = fileURLToPath(new URL("../_build/html", import.meta.url));

/** The SDK's package.json, not the docs toolchain's: its version is the published prefix. */
export const SDK_PACKAGE_JSON = new URL("../../package.json", import.meta.url);

/** Empty string means "served from the root", where root-relative URLs are correct. */
export function normalizeBasePath(rawBasePath) {
  return rawBasePath.replace(/\/+$/, "");
}

/**
 * A blank ``TS_SDK_DOCS_BASE_PATH`` counts as unset rather than as the root, so an empty
 * env var cannot quietly publish a root-relative build past checks that then skip themselves.
 */
export function resolveBasePath(version, override = process.env.TS_SDK_DOCS_BASE_PATH) {
  return normalizeBasePath(override?.trim() || `/docs/ts-sdk/${version}`);
}

export function stripRemoteFontTags(html) {
  return html.replace(REMOTE_FONT_TAG, "");
}

export function hasRemoteFonts(text) {
  return REMOTE_FONTS.test(text);
}

export function inspectPage(html, basePath) {
  return {
    rootRelative: ROOT_RELATIVE.test(html),
    remoteFonts: hasRemoteFonts(html),
    usesBasePath: basePath !== "" && html.includes(`${basePath}/_assets/`),
  };
}

const preview = (files) => `${files.slice(0, 5).join(", ")}${files.length > 5 ? ", ..." : ""}`;

/** @returns {string[]} human-readable failures; empty means the build is publishable. */
export function collectErrors({ basePath, scanned, rootRelative, remoteFonts, usingBasePath }) {
  const errors = [];
  const servedFromRoot = basePath === "";

  if (scanned === 0) return ["no HTML found -- did the TypeDoc build run?"];

  if (!servedFromRoot && rootRelative.length > 0) {
    errors.push(
      `${rootRelative.length} page(s) use root-relative /_assets or /_islands URLs, ` +
        `which resolve against the domain root once published: ${preview(rootRelative)}`,
    );
  }
  if (remoteFonts.length > 0) {
    errors.push(
      `${remoteFonts.length} file(s) load fonts from Google, which ASF privacy guidance ` +
        `does not allow: ${preview(remoteFonts)}`,
    );
  }
  if (!servedFromRoot && usingBasePath === 0) {
    errors.push(`no page references "${basePath}/_assets/" -- basePath is not the published path.`);
  }
  return errors;
}

export async function* filesUnder(dir, extensions) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) yield* filesUnder(path, extensions);
    else if (extensions.some((extension) => entry.name.endsWith(extension))) yield path;
  }
}
