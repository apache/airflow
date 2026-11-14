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

/** The scp-like `git@host:path` SSH remote, which `new URL()` cannot parse. */
const SCP_REMOTE = /^git@([^/:]+):(.+)$/;

/** Split an HTTPS or SSH Git remote into the host its web UI is served on and the repository path. */
function parseGitRemote(repoUrl: string): { host: string; repo: string } | undefined {
  let host: string;
  let path: string;
  const scp = SCP_REMOTE.exec(repoUrl);
  if (scp) {
    [, host = "", path = ""] = scp;
  } else {
    let url: URL;
    try {
      url = new URL(repoUrl);
    } catch {
      return undefined;
    }
    if (url.protocol === "https:") {
      // Keeps a port, as in https://gitlab.corp.com:8443/team/skills.
      host = url.host;
    } else if (url.protocol === "ssh:") {
      // An SSH port is not the web UI's.
      host = url.hostname;
    } else {
      return undefined;
    }
    path = url.pathname.slice(1);
  }
  const repo = path.replace(/\/$/, "").replace(/\.git$/, "");
  return host && repo ? { host, repo } : undefined;
}

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
  const remote = parseGitRemote(source.repo_url);
  const location = remote ? `${remote.host}/${remote.repo}` : source.repo_url;
  const path = source.path === "." ? undefined : source.path;
  if (!remote || commit === undefined) return { location, path, commit: commit?.slice(0, 7) };
  const { host, repo } = remote;
  const treeSegment = host.includes("gitlab") ? "-/tree" : host.includes("github") ? "tree" : undefined;
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
