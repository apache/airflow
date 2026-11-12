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
"""Where fork CI runs: the contributor fork and apache/airflow. Used in dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md and on the ADIP-12 wiki page."""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

DOT = r"""
digraph arch {
  graph [rankdir=LR, fontname="Helvetica", fontsize=11, nodesep=0.4, ranksep=0.7, compound=true, bgcolor="white"];
  node  [shape=box, style="rounded,filled", fillcolor="#f2f4f7", color="#5b6b80", fontname="Helvetica", fontsize=11];
  edge  [fontname="Helvetica", fontsize=9, color="#5b6b80", fontcolor="#33404d"];

  subgraph cluster_fork {
    label="contributor's fork (GitHub-hosted runners, contributor's token)"; style="rounded"; color="#3d8b57"; fontname="Helvetica";
    push   [label="git push feature-branch"];
    ci     [label="Tests (AMD) workflow\npush trigger"];
    gatef  [label="gate job:\nfork -> run"];
    bi     [label="build-info in fork mode\nchanged files vs upstream\n(compare API)"];
    tests  [label="tests and checks\non public runners"];
    push -> ci -> gatef -> bi -> tests;
  }

  subgraph cluster_apache {
    label="apache/airflow"; style="rounded"; color="#5b6b80"; fontname="Helvetica";
    pr     [label="pull_request event"];
    gatea  [label="gate job:\neligibility -> skip\n(build-info and all later jobs skipped)"];
    mon    [label="fork-ci-monitor.yml\n(scheduled, never runs PR code)"];
    rec    [label="breeze ci fork-ci-reconcile"];
    out    [label="per gated PR: draft state, labels,\nstate comment, commit statuses\n(Fork CI / Tests (AMD), CI passed),\nclose after 30 days or on gate tampering"];
    slack  [label="unknown failures -> Slack", fillcolor="#eceff3"];
    pr -> gatea;
    mon -> rec -> out;
    rec -> slack [style=dashed];
  }

  tests -> rec [label="GraphQL: check suites\nREST: runs, jobs, workflow state\n(read by the monitor)", dir=back, style=dashed];
}
"""


def generate_diagram() -> None:
    image_file = MY_DIR / f"{MY_FILENAME}.svg"
    console.print(f"[bright_blue]Generating {image_file}")
    graphviz.Source(DOT).render(outfile=image_file, format="svg", cleanup=True)
    console.print(f"[green]Generated {image_file}")


if __name__ == "__main__":
    generate_diagram()
