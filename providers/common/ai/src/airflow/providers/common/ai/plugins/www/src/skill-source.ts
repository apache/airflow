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

import type { SkillSource } from "src/types/model";

/** An HTTPS or SSH Git remote, split into its host and repository path. */
const GIT_REMOTE = /^(?:https:\/\/|ssh:\/\/git@|git@)([^/:]+)[/:](.+?)(?:\.git)?\/?$/;

/**
 * How a skill's source reads in the tab: the repository or local path, the path inside the
 * repository, the short commit, and a link to that commit's folder when the host is GitHub or
 * GitLab. The link pins the commit, not the branch, so it shows the files the run actually used.
 */
export function describeSkillSource(source: SkillSource): {
  location: string;
  path?: string;
  commit?: string;
  href?: string;
} {
  if (source.repo_url === undefined) return { location: source.path };
  const commit = source.commit ?? undefined;
  const match = GIT_REMOTE.exec(source.repo_url);
  const location = match ? `${match[1]}/${match[2]}` : source.repo_url;
  const path = source.path === "." ? undefined : source.path;
  if (!match || commit === undefined) return { location, path, commit: commit?.slice(0, 7) };
  const [, host, repo] = match;
  const treeSegment = host!.includes("gitlab") ? "-/tree" : host!.includes("github") ? "tree" : undefined;
  const href =
    treeSegment === undefined
      ? undefined
      : `https://${host}/${repo}/${treeSegment}/${commit}${path === undefined ? "" : `/${path}`}`;
  return { location, path, commit: commit.slice(0, 7), href };
}

/** Whether the agent loaded the skill, rather than only seeing it listed. */
export function isLoaded(calls: Record<string, number>): boolean {
  return (calls.load_skill ?? 0) > 0;
}
