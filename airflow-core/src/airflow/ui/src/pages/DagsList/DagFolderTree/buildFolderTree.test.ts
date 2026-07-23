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

import { buildFolderTree, groupFoldersByBundle } from "./buildFolderTree";

describe("buildFolderTree", () => {
  it("returns an empty array for no folders", () => {
    expect(buildFolderTree([])).toEqual([]);
  });

  it("builds single-level roots", () => {
    const tree = buildFolderTree(["team_a", "team_b"]);

    expect(tree.map((node) => node.path)).toEqual(["team_a", "team_b"]);
    expect(tree.every((node) => node.children.length === 0)).toBe(true);
  });

  it("nests sub-folders and keeps full paths", () => {
    const tree = buildFolderTree(["team_a/etl", "team_b/ml"]);

    expect(tree.map((node) => node.path)).toEqual(["team_a", "team_b"]);

    const [teamA] = tree;

    expect(teamA?.name).toBe("team_a");
    expect(teamA?.children).toHaveLength(1);
    expect(teamA?.children[0]?.name).toBe("etl");
    expect(teamA?.children[0]?.path).toBe("team_a/etl");
  });

  it("synthesizes intermediate folders that contain no Dag of their own", () => {
    const tree = buildFolderTree(["team_a/etl/extract"]);

    expect(tree).toHaveLength(1);
    expect(tree[0]?.path).toBe("team_a");
    expect(tree[0]?.children[0]?.path).toBe("team_a/etl");
    expect(tree[0]?.children[0]?.children[0]?.path).toBe("team_a/etl/extract");
  });

  it("merges a folder that is both a leaf and a parent", () => {
    const tree = buildFolderTree(["team_a", "team_a/etl"]);

    expect(tree).toHaveLength(1);
    expect(tree[0]?.path).toBe("team_a");
    expect(tree[0]?.children.map((node) => node.path)).toEqual(["team_a/etl"]);
  });

  it("deduplicates repeated folders", () => {
    const tree = buildFolderTree(["team_a/etl", "team_a/etl"]);

    expect(tree).toHaveLength(1);
    expect(tree[0]?.children).toHaveLength(1);
  });

  it("sorts siblings alphabetically at every level", () => {
    const tree = buildFolderTree(["team_b/zeta", "team_b/alpha", "team_a"]);

    expect(tree.map((node) => node.name)).toEqual(["team_a", "team_b"]);
    const teamB = tree.find((node) => node.name === "team_b");

    expect(teamB?.children.map((node) => node.name)).toEqual(["alpha", "zeta"]);
  });
});

describe("groupFoldersByBundle", () => {
  it("returns an empty array for no folders", () => {
    expect(groupFoldersByBundle([])).toEqual([]);
  });

  it("groups folders under their bundle and sorts bundles alphabetically", () => {
    const bundles = groupFoldersByBundle([
      { bundle_name: "ml", folder: "features" },
      { bundle_name: "analytics", folder: "team_a/etl" },
    ]);

    expect(bundles.map((bundle) => bundle.bundleName)).toEqual(["analytics", "ml"]);
  });

  it("keeps the same folder path separate per bundle", () => {
    const bundles = groupFoldersByBundle([
      { bundle_name: "analytics", folder: "team_a/etl" },
      { bundle_name: "ml", folder: "team_a/etl" },
    ]);

    const analytics = bundles.find((bundle) => bundle.bundleName === "analytics");
    const ml = bundles.find((bundle) => bundle.bundleName === "ml");

    expect(analytics?.tree[0]?.children[0]?.path).toBe("team_a/etl");
    expect(ml?.tree[0]?.children[0]?.path).toBe("team_a/etl");
  });

  it("builds a full folder tree within each bundle", () => {
    const bundles = groupFoldersByBundle([
      { bundle_name: "b1", folder: "team_a/etl" },
      { bundle_name: "b1", folder: "team_a/report" },
    ]);

    expect(bundles).toHaveLength(1);
    expect(bundles[0]?.tree[0]?.path).toBe("team_a");
    expect(bundles[0]?.tree[0]?.children.map((node) => node.name)).toEqual(["etl", "report"]);
  });
});
