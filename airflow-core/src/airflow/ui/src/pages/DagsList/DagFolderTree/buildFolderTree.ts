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
export type FolderNode = {
  /** Direct child folders, keyed alphabetically by their display name. */
  readonly children: Array<FolderNode>;
  /** Last path segment, shown in the tree (e.g. ``etl``). */
  readonly name: string;
  /** Full folder path from the bundle root (e.g. ``team_a/etl``), used as the filter value. */
  readonly path: string;
};

/**
 * Build a nested folder tree from a flat list of folder paths.
 *
 * The backend only returns the directory of each Dag file (leaf folders), so intermediate
 * folders that contain no Dag of their own — but do contain sub-folders — are synthesized
 * here. Given ``["team_a/etl", "team_b/ml"]`` the ``team_a``/``team_b`` nodes are created even
 * though no Dag lives directly in them.
 *
 * Children are sorted alphabetically at every level for a stable, predictable rendering.
 */
export const buildFolderTree = (folders: ReadonlyArray<string>): Array<FolderNode> => {
  type MutableNode = { children: Map<string, MutableNode>; name: string; path: string };

  const roots = new Map<string, MutableNode>();

  for (const folder of folders) {
    const segments = folder.split("/").filter((segment) => segment !== "");

    let level = roots;
    let prefix = "";

    for (const segment of segments) {
      prefix = prefix === "" ? segment : `${prefix}/${segment}`;

      let node = level.get(segment);

      if (node === undefined) {
        node = { children: new Map(), name: segment, path: prefix };
        level.set(segment, node);
      }

      level = node.children;
    }
  }

  const toSortedNodes = (level: Map<string, MutableNode>): Array<FolderNode> =>
    [...level.values()]
      .sort((left, right) => left.name.localeCompare(right.name))
      .map((node) => ({
        children: toSortedNodes(node.children),
        name: node.name,
        path: node.path,
      }));

  return toSortedNodes(roots);
};
