#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use it except in compliance
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
"""Build dependency graph from skills.json (prereqs) and output tree + JSON graph."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def parse_prereq_skill_ids(prereqs_str: str, skill_ids: set[str]) -> list[str]:
    """Return list of skill IDs that appear in prereqs string (comma-separated)."""
    if not prereqs_str or not prereqs_str.strip():
        return []
    return [x.strip() for x in prereqs_str.split(",") if x.strip() in skill_ids]


def find_cycle(skill_ids: set[str], edges: list[tuple[str, str]]) -> list[str] | None:
    """edges are (from_id, to_id) meaning from_id requires to_id. Return cycle as list or None."""
    # Build adjacency: node -> list of nodes it points to (dependencies)
    adj: dict[str, list[str]] = {sid: [] for sid in skill_ids}
    for from_id, to_id in edges:
        adj[from_id].append(to_id)
    path: list[str] = []
    path_set: set[str] = set()
    in_stack: set[str] = set()

    def visit(n: str) -> list[str] | None:
        path.append(n)
        path_set.add(n)
        in_stack.add(n)
        for m in adj.get(n, []):
            if m not in path_set:
                cycle = visit(m)
                if cycle is not None:
                    return cycle
            elif m in in_stack:
                # cycle: from m back to m
                start = path.index(m)
                return path[start:] + [m]
        path.pop()
        path_set.remove(n)
        in_stack.remove(n)
        return None

    for sid in skill_ids:
        if sid not in path_set:
            cycle = visit(sid)
            if cycle is not None:
                return cycle
    return None


def build_graph(skills: list[dict]) -> tuple[list[dict], list[dict], dict[str, dict]]:
    """
    Build nodes (for JSON), edges (for JSON), and id->skill for tree.
    Edges: from skill_id to prereq_id (skill_id requires prereq_id).
    """
    skill_ids = {s["id"] for s in skills}
    id_to_skill = {s["id"]: s for s in skills}
    nodes = [
        {"id": s["id"], "category": s.get("category", ""), "context": s.get("context", "")}
        for s in skills
    ]
    edges = []
    for s in skills:
        sid = s["id"]
        prereqs = parse_prereq_skill_ids(s.get("prereqs", "") or "", skill_ids)
        for p in prereqs:
            edges.append({"from": sid, "to": p, "type": "requires"})
    return nodes, edges, id_to_skill


def build_tree_edges(id_to_skill: dict[str, dict], edges: list[dict]) -> dict[str, list[str]]:
    """Given edges (from->to = requires), return for each node the list of children (dependents)."""
    # edges: from A to B means A requires B. So B is dependency of A. Children of B = skills that require B.
    children: dict[str, list[str]] = {sid: [] for sid in id_to_skill}
    for e in edges:
        child_id = e["from"]
        parent_id = e["to"]
        children[parent_id].append(child_id)
    return children


def main() -> int:
    parser = argparse.ArgumentParser(description="Build skill dependency graph from skills.json")
    parser.add_argument(
        "--skills-json",
        default=None,
        help="Path to skills.json (default: contributing-docs/agent_skills/skills.json under repo root)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to write skill_graph.json (default: contributing-docs/agent_skills/skill_graph.json)",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    skills_path = Path(args.skills_json) if args.skills_json else root / "contributing-docs/agent_skills/skills.json"
    if not skills_path.exists():
        print("skills.json not found", file=sys.stderr)
        return 1
    out_path = Path(args.output) if args.output else root / "contributing-docs/agent_skills/skill_graph.json"
    data = json.loads(skills_path.read_text(encoding="utf-8"))
    skills = data.get("skills", [])
    if not skills:
        print("Skill Dependency Graph")
        print("=====================")
        print("(no skills)")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps({"nodes": [], "edges": []}, indent=2), encoding="utf-8")
        return 0

    nodes, edges, id_to_skill = build_graph(skills)
    # Cycle check: edges are (from, to) = from requires to. So graph: from -> to. Cycle = path from A to A.
    edge_tuples = [(e["from"], e["to"]) for e in edges]
    cycle = find_cycle(set(id_to_skill), edge_tuples)
    if cycle is not None:
        print(f"Circular dependency detected: {' -> '.join(cycle)}", file=sys.stderr)
        return 1

    # Roots = skills with no skill prereqs (prereqs string contains no other skill id).
    skill_ids = set(id_to_skill)
    roots = [s["id"] for s in skills if not parse_prereq_skill_ids(s.get("prereqs", "") or "", skill_ids)]
    roots = sorted(roots) if roots else sorted(id_to_skill)

    children = build_tree_edges(id_to_skill, edges)
    # Sort children for stable tree
    for sid in children:
        children[sid] = sorted(children[sid])

    lines = ["Skill Dependency Graph", "====================="]
    # First line: root without prefix
    for rid in roots:
        s = id_to_skill[rid]
        lines.append(f"{rid}  [{s.get('category', '')}] [{s.get('context', '')}]")
        for i, cid in enumerate(children.get(rid, [])):
            branch = "\\-- " if i == len(children.get(rid, [])) - 1 else "+-- "
            sub_next = "    " if i == len(children.get(rid, [])) - 1 else "|   "
            sub_lines = _subtree_lines(cid, id_to_skill, children, branch, sub_next)
            lines.extend(sub_lines)
    for line in lines:
        print(line)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"nodes": nodes, "edges": edges}, indent=2), encoding="utf-8")
    return 0


def _subtree_lines(
    node_id: str,
    id_to_skill: dict[str, dict],
    children: dict[str, list[str]],
    prefix: str,
    next_prefix: str,
) -> list[str]:
    s = id_to_skill[node_id]
    line = f"{prefix}{node_id}  [{s.get('category', '')}] [{s.get('context', '')}]"
    kid_list = children.get(node_id, [])
    result = [line]
    for i, kid in enumerate(kid_list):
        branch = "\\-- " if i == len(kid_list) - 1 else "+-- "
        sub_next = "    " if i == len(kid_list) - 1 else "|   "
        result.extend(_subtree_lines(kid, id_to_skill, children, next_prefix + branch, next_prefix + sub_next))
    return result


if __name__ == "__main__":
    sys.exit(main())
