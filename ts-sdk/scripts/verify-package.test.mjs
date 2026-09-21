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

import { describe, expect, it } from "vitest";
import {
  REQUIRED_ROOT_EXPORTS,
  buildImportScript,
  collectEntryPoints,
  collectExportSubpaths,
  diffPackagedFiles,
  isAllowedFile,
  parsePackMetadata,
} from "./verify-package.mjs";

const PACKAGE_JSON = {
  name: "apache-airflow-ts-sdk",
  exports: {
    ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
    "./coordinator": {
      types: "./dist/coordinator/index.d.ts",
      import: "./dist/coordinator/index.js",
    },
  },
  bin: { "airflow-ts-pack": "./dist/cli/main.js" },
};

describe("collectEntryPoints", () => {
  it("flattens every exports condition and bin target", () => {
    expect(collectEntryPoints(PACKAGE_JSON)).toEqual([
      "dist/index.d.ts",
      "dist/index.js",
      "dist/coordinator/index.d.ts",
      "dist/coordinator/index.js",
      "dist/cli/main.js",
    ]);
  });

  it("accepts a bare string export target", () => {
    expect(collectEntryPoints({ exports: { ".": "./dist/index.js" } })).toEqual(["dist/index.js"]);
  });

  it("returns nothing when a package declares neither exports nor bin", () => {
    expect(collectEntryPoints({})).toEqual([]);
  });
});

describe("collectExportSubpaths", () => {
  it("maps the root export to a bare specifier and keeps subpaths", () => {
    expect(collectExportSubpaths(PACKAGE_JSON)).toEqual(["", "/coordinator"]);
  });

  it("assumes a root export when exports is absent", () => {
    expect(collectExportSubpaths({})).toEqual([""]);
  });
});

describe("isAllowedFile", () => {
  it("allows the required root files and compiled dist output", () => {
    for (const path of [
      "LICENSE",
      "NOTICE",
      "README.md",
      "package.json",
      "dist/index.js",
      "dist/index.d.ts",
      "dist/cli/main.js",
    ]) {
      expect(isAllowedFile(path)).toBe(true);
    }
  });

  it("rejects sources, source maps, and stray files", () => {
    for (const path of [
      "src/index.ts",
      "dist/index.js.map",
      "dist/index.d.ts.map",
      "dist/notes.txt",
      "dist/",
      ".npmrc",
      "tests/public-api.test.ts",
    ]) {
      expect(isAllowedFile(path)).toBe(false);
    }
  });
});

describe("parsePackMetadata", () => {
  it("takes the trailing JSON object after the prepack banner", () => {
    const stdout = [
      "> apache-airflow-ts-sdk@0.1.0 prepack",
      "> pnpm run build",
      "",
      '{"name":"apache-airflow-ts-sdk","filename":"pkg.tgz","files":[{"path":"LICENSE"}]}',
      "",
    ].join("\n");
    expect(parsePackMetadata(stdout).filename).toBe("pkg.tgz");
  });

  it("rejects stdout that carries no metadata object", () => {
    expect(() => parsePackMetadata("> pnpm run build\n")).toThrow(
      "pnpm pack did not return package metadata",
    );
  });

  it("rejects metadata missing the files list or the filename", () => {
    for (const payload of ['{"filename":"pkg.tgz"}', '{"files":[]}']) {
      expect(() => parsePackMetadata(payload)).toThrow(
        "pnpm pack returned invalid package metadata",
      );
    }
  });
});

describe("diffPackagedFiles", () => {
  it("reports nothing for a tarball holding exactly the allowed paths", () => {
    const paths = new Set([
      "LICENSE",
      "NOTICE",
      "README.md",
      "package.json",
      ...collectEntryPoints(PACKAGE_JSON),
    ]);
    expect(diffPackagedFiles(PACKAGE_JSON, paths)).toEqual({ missing: [], unexpected: [] });
  });

  it("reports required paths that are absent and disallowed paths that are present", () => {
    const paths = new Set(["LICENSE", "NOTICE", "README.md", "package.json", "src/index.ts"]);
    const { missing, unexpected } = diffPackagedFiles(PACKAGE_JSON, paths);
    expect(missing).toEqual(collectEntryPoints(PACKAGE_JSON));
    expect(unexpected).toEqual(["src/index.ts"]);
  });
});

describe("buildImportScript", () => {
  it("imports every exports subpath before asserting the root entrypoints", () => {
    const script = buildImportScript(PACKAGE_JSON);
    expect(script).toContain('await import("apache-airflow-ts-sdk");');
    expect(script).toContain('await import("apache-airflow-ts-sdk/coordinator");');
    for (const name of REQUIRED_ROOT_EXPORTS) {
      expect(script).toContain(name);
    }
  });

  it("produces a module body that parses", () => {
    expect(
      () => new Function(`return async () => {${buildImportScript(PACKAGE_JSON)}}`),
    ).not.toThrow();
  });
});
