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
# requires-python = ">=3.10"
# dependencies = [
#    "rich>=13.6.0",
#    "graphviz>=0.20.1",
# ]
# ///
"""
Worker process model for the *Architecture Overview* page.

Answers the one question a Deployment Manager needs in order to size a worker:
*what runs, and for how long, while a task instance executes?* Every task
instance gets its own subprocess whatever language it is written in, and that
subprocess lives exactly as long as the task instance — for Java and Go it is a
fresh JVM / fresh binary process each time, not a pooled runtime.

The worker process's own lifetime is deliberately left unstated: it is pooled and
reused under the Local and Celery executors, but one-shot per task instance under
the Kubernetes executor. Only the task subprocess behaves the same way everywhere.

Deliberately omits how the worker and the subprocess talk to each other
(coordinators, the msgpack comm frame, the Execution API); that detail is
developer-facing and lives in ``contributing-docs/31_task_execution_architecture.rst``.
"""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

# (fill, border) per role — consistent with the detailed Task SDK diagrams.
WORKER = ("#e3f2fd", "#1565c0")  # the long-running worker process (blue)
PY = ("#e8f5e9", "#2e7d32")  # Python task subprocess (green)
LANG = ("#fbe9e7", "#d84315")  # JVM / native binary task subprocess (deep orange)

# One box per task instance: (node id, heading, what the subprocess actually is, theme).
TASK_INSTANCES = (
    ("py", "Task instance   ·   Python", "a forked Python interpreter", PY),
    ("java", "Task instance   ·   Java", "a brand-new JVM instance", LANG),
    ("go", "Task instance   ·   Go", "a brand-new process of the compiled binary", LANG),
)


def _label(title: str, sub: str | None = None) -> str:
    html = f"<<b>{title}</b>"
    if sub:
        html += f'<br/><font point-size="11" color="#37474f">{sub}</font>'
    return html + ">"


def _node(g, node_id: str, title: str, sub: str, *, shape: str, theme: tuple[str, str]) -> None:
    fill, border = theme
    style = "filled" if shape == "box3d" else "rounded,filled"
    g.node(
        node_id,
        label=_label(title, sub),
        shape=shape,
        style=style,
        fillcolor=fill,
        color=border,
        penwidth="2",
        margin="0.20,0.14",
    )


def generate_task_execution_architecture_diagram():
    image_file = MY_DIR / f"{MY_FILENAME}.png"
    console.print(f"[bright_blue]Generating architecture image {image_file}")

    g = graphviz.Digraph("task_execution_architecture")
    g.attr(
        rankdir="TB",
        splines="spline",
        nodesep="0.7",
        ranksep="1.1",
        pad="0.5",
        bgcolor="white",
        fontname="Helvetica",
        newrank="true",
    )
    g.attr("node", fontname="Helvetica", fontsize="13", fontcolor="#102027")
    g.attr("edge", fontname="Helvetica", fontsize="11", penwidth="1.8", color="#546e7a")

    with g.subgraph(name="cluster_worker") as worker:
        worker.attr(
            label="Worker",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#fafafa",
            color="#607d8b",
            penwidth="1.8",
            fontsize="19",
            fontname="Helvetica-Bold",
            margin="24",
        )
        _node(
            worker,
            "worker",
            "Worker process",
            "supervises the task instance   ·   holds its credentials   ·   runs NO user code",
            shape="box",
            theme=WORKER,
        )

        for node_id, heading, subprocess_kind, theme in TASK_INSTANCES:
            _node(
                worker,
                node_id,
                heading,
                f"{subprocess_kind}   ·   runs USER CODE",
                shape="box3d",
                theme=theme,
            )

    for index, (node_id, _, _, theme) in enumerate(TASK_INSTANCES):
        g.edge(
            "worker",
            node_id,
            color=theme[1],
            arrowhead="vee",
            # Label the middle edge only — all three edges mean the same thing, and repeating
            # the text three times crowds the fan-out.
            label="starts a new subprocess\nper task instance" if index == 1 else "",
        )

    g.render(outfile=str(image_file), format="png", cleanup=True)
    console.print(f"[green]Generated architecture image {image_file}")


if __name__ == "__main__":
    generate_task_execution_architecture_diagram()
