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
import { test } from "node:test";

import { describeSkillSource, isLoaded } from "../src/skill-source.ts";

const commit = "0123456789abcdef";

test("a local skill reads as its path, with no link", () => {
  assert.deepEqual(describeSkillSource({ path: "/opt/airflow/skills/alpha" }), {
    location: "/opt/airflow/skills/alpha",
  });
});

for (const [repoUrl, href] of [
  ["https://github.com/org/skills.git", `https://github.com/org/skills/tree/${commit}/skills/alpha`],
  ["git@github.com:org/skills.git", `https://github.com/org/skills/tree/${commit}/skills/alpha`],
  ["ssh://git@gitlab.com/group/sub/skills", `https://gitlab.com/group/sub/skills/-/tree/${commit}/skills/alpha`],
]) {
  test(`a Git skill from ${repoUrl} links the folder at the commit that ran`, () => {
    assert.deepEqual(describeSkillSource({ repo_url: repoUrl, path: "skills/alpha", branch: "main", commit }), {
      location: repoUrl.includes("gitlab") ? "gitlab.com/group/sub/skills" : "github.com/org/skills",
      path: "skills/alpha",
      commit: "0123456",
      href,
    });
  });
}

test("a skill at the repository root links the commit's root", () => {
  const source = { repo_url: "https://github.com/org/skills", path: ".", commit };

  assert.equal(describeSkillSource(source).href, `https://github.com/org/skills/tree/${commit}`);
});

test("a host with no known web UI shows the repository and commit without a link", () => {
  const source = { repo_url: "https://git.example.com/org/skills.git", path: "skills/alpha", commit };

  assert.deepEqual(describeSkillSource(source), {
    location: "git.example.com/org/skills",
    path: "skills/alpha",
    commit: "0123456",
    href: undefined,
  });
});

test("only a skill the agent loaded counts as loaded", () => {
  assert.equal(isLoaded({ load_skill: 1 }), true);
  assert.equal(isLoaded({ read_skill_resource: 2 }), false);
  assert.equal(isLoaded({}), false);
});
