#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Query the Agent Skills manifest for contributors and AI agents."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

SKILLS_JSON = pathlib.Path("contributing-docs/agent_skills/skills.json")
GRAPH_JSON = pathlib.Path("contributing-docs/agent_skills/skill_graph.json")


def load_skills() -> list[dict]:
    with SKILLS_JSON.open() as f:
        return json.load(f)["skills"]


def load_graph() -> dict:
    with GRAPH_JSON.open() as f:
        return json.load(f)


def get_prereq_chain(skill_id: str, skills_by_id: dict[str, dict]) -> list[str]:
    """Return the full execution chain for a skill in correct order (prereqs first)."""
    chain: list[str] = []
    visited: set[str] = set()

    def visit(sid: str) -> None:
        if sid in visited:
            return
        visited.add(sid)
        skill = skills_by_id.get(sid)
        if not skill:
            return
        prereqs = skill.get("prereqs", "")
        if prereqs:
            for raw in prereqs.split(","):
                dep = raw.strip()
                if dep and dep in skills_by_id:
                    visit(dep)
        chain.append(sid)

    visit(skill_id)
    return chain


def main() -> int:
    parser = argparse.ArgumentParser(description="Query Agent Skills manifest")
    parser.add_argument("--context", help="Filter by context (host/breeze/either)")
    parser.add_argument("--category", help="Filter by category")
    parser.add_argument("--chain", help="Show full execution chain for a skill")
    parser.add_argument("--skill", help="Show details for a specific skill")
    args = parser.parse_args()

    skills = load_skills()
    skills_by_id = {s["id"]: s for s in skills}

    if args.skill:
        skill = skills_by_id.get(args.skill)
        if not skill:
            print(f"Skill '{args.skill}' not found")
            return 1
        print(json.dumps(skill, indent=2))
        return 0

    if args.chain:
        if args.chain not in skills_by_id:
            print(f"Skill '{args.chain}' not found")
            return 1
        chain = get_prereq_chain(args.chain, skills_by_id)
        print(f"Execution chain for '{args.chain}':")
        for index, sid in enumerate(chain, 1):
            skill = skills_by_id[sid]
            marker = "->" if index < len(chain) else "[OK]"
            print(
                f"  {index}. {marker} {sid} "
                f"[{skill.get('context', '')}] "
                f"— {skill.get('description', '')}",
            )
        return 0

    filtered = list(skills)
    if args.context:
        filtered = [s for s in filtered if s.get("context") in (args.context, "either")]
    if args.category:
        filtered = [s for s in filtered if s.get("category") == args.category]

    print(f"Skills ({len(filtered)}):")
    for skill in filtered:
        print(
            f"  {skill['id']:<35} "
            f"[{skill.get('context', ''):<6}] "
            f"[{skill.get('category', '')}]",
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

