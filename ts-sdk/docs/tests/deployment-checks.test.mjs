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

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  collectErrors,
  inspectPage,
  normalizeBasePath,
  resolveBasePath,
  stripRemoteFontTags,
} from "../scripts/deployment-checks.mjs";

// Verbatim from a clean-jsdoc-theme build: three tags together, with the query separators
// HTML-escaped the way the theme escapes the href it builds.
const FONT_TAGS =
  '<link rel="preconnect" href="https://fonts.googleapis.com" />' +
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />' +
  '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Source+Serif+4:wght@400;500;600;700&amp;family=Roboto:wght@400;500;600;700&amp;display=swap" />';

const BASE = "/docs/ts-sdk/0.1.0-beta1";
const VERSION = "0.1.0-beta1";

describe("normalizeBasePath", () => {
  it("strips trailing slashes", () => {
    assert.equal(normalizeBasePath(`${BASE}/`), BASE);
    assert.equal(normalizeBasePath(BASE), BASE);
  });

  it("maps the root to an empty string", () => {
    assert.equal(normalizeBasePath("/"), "");
    assert.equal(normalizeBasePath("//"), "");
  });
});

describe("resolveBasePath", () => {
  it("defaults to the published prefix for the SDK version", () => {
    assert.equal(resolveBasePath(VERSION, undefined), BASE);
  });

  it("honours an override, normalized", () => {
    assert.equal(resolveBasePath(VERSION, "/preview/"), "/preview");
    assert.equal(resolveBasePath(VERSION, "/"), "");
  });

  // A blank env var is how the override leaks in unintentionally, and treating it as the
  // root would both emit root-relative HTML and make the checks skip themselves.
  it("treats a blank override as unset", () => {
    assert.equal(resolveBasePath(VERSION, ""), BASE);
    assert.equal(resolveBasePath(VERSION, "   "), BASE);
  });
});

describe("stripRemoteFontTags", () => {
  it("removes preconnect and stylesheet tags", () => {
    assert.equal(stripRemoteFontTags(`<head>${FONT_TAGS}</head>`), "<head></head>");
  });

  it("keeps local stylesheets", () => {
    const local = `<link rel="stylesheet" href="${BASE}/_assets/styles.css" />`;
    assert.equal(stripRemoteFontTags(`<head>${FONT_TAGS}${local}</head>`), `<head>${local}</head>`);
  });

  it("is a no-op on already-clean HTML", () => {
    const html = "<head><title>x</title></head>";
    assert.equal(stripRemoteFontTags(html), html);
  });
});

describe("inspectPage", () => {
  it("flags root-relative asset and island URLs", () => {
    assert.equal(inspectPage('<link href="/_assets/styles.css" />', BASE).rootRelative, true);
    assert.equal(inspectPage('<script src="/_islands/cmdk.js">', BASE).rootRelative, true);
  });

  it("accepts prefixed URLs", () => {
    const page = inspectPage(`<link href="${BASE}/_assets/styles.css" />`, BASE);
    assert.equal(page.rootRelative, false);
    assert.equal(page.usesBasePath, true);
  });

  it("does not credit a basePath from another version", () => {
    const page = inspectPage('<link href="/docs/ts-sdk/9.9.9/_assets/styles.css" />', BASE);
    assert.equal(page.rootRelative, false);
    assert.equal(page.usesBasePath, false);
  });

  it("never credits a basePath when served from the root", () => {
    assert.equal(inspectPage('<link href="/_assets/styles.css" />', "").usesBasePath, false);
  });

  it("detects both font hosts", () => {
    assert.equal(inspectPage(FONT_TAGS, BASE).remoteFonts, true);
    assert.equal(inspectPage('<link href="https://fonts.gstatic.com/x" />', BASE).remoteFonts, true);
    assert.equal(inspectPage("<head></head>", BASE).remoteFonts, false);
  });
});

describe("collectErrors", () => {
  const clean = {
    basePath: BASE,
    scanned: 36,
    rootRelative: [],
    remoteFonts: [],
    usingBasePath: 36,
  };

  it("passes a correct build", () => {
    assert.deepEqual(collectErrors(clean), []);
  });

  it("reports an empty build before anything else", () => {
    const errors = collectErrors({ ...clean, scanned: 0, usingBasePath: 0 });
    assert.equal(errors.length, 1);
    assert.match(errors[0], /did the TypeDoc build run/);
  });

  it("reports every failure of the unfixed build at once", () => {
    const errors = collectErrors({
      ...clean,
      rootRelative: ["index.html"],
      remoteFonts: ["index.html"],
      usingBasePath: 0,
    });
    assert.equal(errors.length, 3);
    assert.match(errors[0], /root-relative/);
    assert.match(errors[1], /ASF privacy guidance/);
    assert.match(errors[2], /basePath is not the published path/);
  });

  it("truncates long file lists", () => {
    const files = Array.from({ length: 36 }, (_, i) => `page-${i}.html`);
    const [error] = collectErrors({ ...clean, remoteFonts: files });
    assert.match(error, /^36 file\(s\) load fonts/);
    assert.match(error, /page-4\.html, \.\.\.$/);
    assert.equal(error.includes("page-5.html"), false);
  });

  it("skips the prefix checks when served from the root", () => {
    const rooted = { ...clean, basePath: "", rootRelative: ["index.html"], usingBasePath: 0 };
    assert.deepEqual(collectErrors(rooted), []);
    assert.equal(collectErrors({ ...rooted, remoteFonts: ["index.html"] }).length, 1);
  });
});
