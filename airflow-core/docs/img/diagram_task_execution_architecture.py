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
High-level task execution architecture for the *Architecture Overview* page.

Deliberately coarse-grained: it shows only the process boundaries a Deployment
Manager needs to reason about — the **Supervisor** process, the
``CoordinatorManager`` that routes a task to a coordinator by its
``TaskInstance.queue``, and the **language SDK subprocess** the coordinator
launches for non-Python tasks. Internal plumbing (subprocess drivers, TCP port
setup, schema versioning) is intentionally omitted; that detail lives in the
developer guide under ``contributing-docs/`` instead.
"""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

# (fill, border) per role — consistent with the detailed Task SDK diagrams.
SUP = ("#e3f2fd", "#1565c0")  # Supervisor process (blue)
COORD = ("#ede7f6", "#5e35b1")  # Coordinator layer (deep purple)
PY = ("#e8f5e9", "#2e7d32")  # built-in Python coordinator (green)
LANG = ("#fbe9e7", "#d84315")  # language SDK runtime — JVM or compiled binary (deep orange)


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
        margin="0.18,0.12",
    )


def generate_task_execution_architecture_diagram():
    image_file = MY_DIR / f"{MY_FILENAME}.png"
    console.print(f"[bright_blue]Generating architecture image {image_file}")

    g = graphviz.Digraph("task_execution_architecture")
    g.attr(
        rankdir="TB",
        splines="spline",
        nodesep="0.6",
        ranksep="1.0",
        pad="0.5",
        bgcolor="white",
        fontname="Helvetica",
        newrank="true",
        compound="true",
    )
    g.attr("node", fontname="Helvetica", fontsize="13", fontcolor="#102027")
    g.attr("edge", fontname="Helvetica", fontsize="10", penwidth="1.8", color="#546e7a")

    with g.subgraph(name="cluster_worker") as worker:
        worker.attr(
            label="Worker",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#f5f5f5",
            color="#607d8b",
            penwidth="1.8",
            fontsize="19",
            fontname="Helvetica-Bold",
            margin="24",
        )

        with worker.subgraph(name="cluster_sup") as sup:
            sup.attr(
                label="Supervisor process   ·   native OS process (Python)",
                labelloc="t",
                style="rounded,filled",
                fillcolor="#eef0fb",
                color=SUP[1],
                penwidth="1.5",
                fontsize="15",
                fontname="Helvetica-Bold",
                margin="16",
            )
            _node(
                sup,
                "coord_mgr",
                "CoordinatorManager",
                "matches TaskInstance.queue → a coordinator",
                shape="box",
                theme=SUP,
            )
            _node(
                sup,
                "pycoord",
                "Python Coordinator   (default)",
                "",
                shape="box",
                theme=PY,
            )
            _node(
                sup,
                "java",
                "Java Coordinator",
                "JAR bundles",
                shape="box",
                theme=COORD,
            )
            _node(
                sup,
                "exe",
                "Executable Coordinator",
                "self-contained native bundles (e.g. Go)",
                shape="box",
                theme=COORD,
            )
            sup.edge(
                "coord_mgr", "pycoord", style="dotted", color=PY[1], arrowhead="vee", label="default queue"
            )
            sup.edge("coord_mgr", "java", style="dotted", color=COORD[1], arrowhead="vee", label="java queue")
            sup.edge("coord_mgr", "exe", style="dotted", color=COORD[1], arrowhead="vee", label="other queue")

        _node(
            worker,
            "jvm",
            "JVM subprocess",
            "each task-instance launches a new JVM instance   ·   runs USER CODE",
            shape="box3d",
            theme=LANG,
        )
        _node(
            worker,
            "gobin",
            "Go subprocess",
            "each task-instance launches a new native binary executable   ·   runs USER CODE",
            shape="box3d",
            theme=LANG,
        )
        _node(
            worker,
            "pysub",
            "Python subprocess",
            "runs the Python workload with the Task SDK<br/>execution_time task_runner   ·   runs USER CODE",
            shape="box3d",
            theme=PY,
        )

    g.edge("pycoord", "pysub", color=PY[1], arrowhead="vee", label="fork")
    g.edge("java", "jvm", color=COORD[1], arrowhead="vee", label="launch")
    g.edge("exe", "gobin", color=COORD[1], arrowhead="vee", label="launch")
    # The comm frame is a channel to the Supervisor process, so clip the head at the
    # supervisor cluster boundary (lhead); routing toward each subprocess's own coordinator
    # keeps the arrow local and vertical instead of crossing the diagram.
    for coord, subproc in (("pycoord", "pysub"), ("java", "jvm"), ("exe", "gobin")):
        g.edge(
            subproc,
            coord,
            lhead="cluster_sup",
            label="MsgPack comm frame",
            color=LANG[1],
            fontcolor=LANG[1],
            dir="both",
            penwidth="2.2",
        )

    g.render(outfile=str(image_file), format="png", cleanup=True)
    console.print(f"[green]Generated architecture image {image_file}")


if __name__ == "__main__":
    generate_task_execution_architecture_diagram()
