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
# /// script
# requires-python = ">=3.11"
# dependencies = [
#    "rich>=13.6.0",
#    "graphviz>=0.20.1",
# ]
# ///
"""What a gated pull request shows, and who acts next, from opening to merge or close. Used in dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md and on the ADIP-12 wiki page."""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

DOT = r"""
digraph lifecycle {
  graph [rankdir=TB, fontname="Helvetica", fontsize=11, nodesep=0.5, ranksep=0.55, bgcolor="white"];
  node  [shape=box, style="rounded,filled", fillcolor="#f2f4f7", color="#5b6b80", fontname="Helvetica", fontsize=11, margin="0.15,0.08"];
  edge  [fontname="Helvetica", fontsize=9, color="#5b6b80", fontcolor="#33404d"];

  start [shape=circle, label="", width=0.22, fillcolor="#33404d", color="#33404d"];
  done  [shape=doublecircle, label="", width=0.18, fillcolor="#33404d", color="#33404d"];

  waiting [label="Waiting for the author\n(draft: fork CI not set up,\nor no run for the head commit)"];
  running [label="Fork CI running"];
  failed  [label="Fork CI failed\n(out of the review queue)", fillcolor="#fde8e8", color="#b54848"];
  ready   [label="Green\nready for maintainer review\nCI passed", fillcolor="#e6f4ea", color="#3d8b57"];
  rebase  [label="Green, base too old\nready for maintainer review\nCI passed pending until a rebase", fillcolor="#fff6db", color="#b38a1d"];
  needs   [label="Green, CI files changed\nneeds project ci", fillcolor="#fff6db", color="#b38a1d"];
  project [label="Project CI\n(use project ci)"];
  approved[label="Approved"];
  merged  [label="Merged", fillcolor="#e6f4ea", color="#3d8b57"];
  closed  [label="Closed after 30 days\nwithout fork CI activity", fillcolor="#eceff3", color="#8a96a3"];

  start -> waiting [label="external contributor\nopens a pull request"];
  waiting -> running [label="author enables the fork\nworkflow and pushes"];
  running -> failed [label="a job fails, times out,\nis cancelled, or the\nrun cannot start"];
  running -> ready [label="passes on a\ncurrent base"];
  running -> rebase [label="passes, base 200+ commits\nbehind or changed\nthe same files"];
  running -> needs [label="passes, changes\nCI-defining files"];
  failed -> running [label="author pushes a fix"];
  rebase -> running [label="author rebases\nand pushes"];
  ready -> running [label="author pushes", style=dashed];
  ready -> approved [label="maintainer approves"];
  rebase -> approved [label="maintainer approves;\nmerge waits for the rebase", style=dashed];
  ready -> project [label="maintainer adds\nuse project ci", style=dashed];
  needs -> project [label="maintainer reviews the\nCI change, adds use project ci"];
  project -> approved [label="project CI passes,\nmaintainer approves"];
  approved -> merged [label="merged, or auto-merged\nonce CI passed is green"];
  waiting -> closed [label="30 days without\nfork CI activity"];
  closed -> waiting [label="author reopens", style=dashed];
  merged -> done;
}
"""


def generate_diagram() -> None:
    image_file = MY_DIR / f"{MY_FILENAME}.svg"
    console.print(f"[bright_blue]Generating {image_file}")
    graphviz.Source(DOT).render(outfile=image_file, format="svg", cleanup=True)
    console.print(f"[green]Generated {image_file}")


if __name__ == "__main__":
    generate_diagram()
