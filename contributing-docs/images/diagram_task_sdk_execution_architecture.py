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
Architecture diagram for Task SDK execution.

Contrasts the two ways a task is run:

* the *supervised* path used in production, where the Supervisor ("Coordinator")
  and the Task each run in their own **native OS process** (Python interpreter)
  and talk over a socket, with the Supervisor proxying the remote Execution API; and
* the *native / in-process* path (``dag.test()`` and local runs), where the whole
  thing runs **inside a single Python process** with in-memory queues instead of
  sockets and an in-memory Execution API instead of HTTP.

Rendered with graphviz directly (rather than the ``diagrams`` library) so every
label sits *inside* a sized shape — nothing overlaps, and each kind of thing gets
its own shape: 3-D box = native OS process, rounded box = an object inside a
process, component = an ASGI/FastAPI app, cylinder = the database.
"""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

# Soft, modern palette: (fill, border) per role.
TASK = ("#e8f5e9", "#2e7d32")  # Task SDK / user code
SUP = ("#e3f2fd", "#1565c0")  # Supervisor ("Coordinator")
API = ("#fdecea", "#c62828")  # Execution API
NATIVE = ("#f3e5f5", "#7b1fa2")  # in-process "native" path
NEUTRAL = ("#eceff1", "#455a64")  # database


def _label(title: str, sub: str | None = None) -> str:
    """Build an HTML-like label: bold title over a smaller, dimmer subtitle."""
    html = f"<<b>{title}</b>"
    if sub:
        html += f'<br/><font point-size="11" color="#37474f">{sub}</font>'
    return html + ">"


def _node(g, node_id: str, title: str, sub: str, *, shape: str, theme: tuple[str, str]) -> None:
    fill, border = theme
    style = "filled" if shape in ("box3d", "cylinder", "component") else "rounded,filled"
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


def generate_task_sdk_execution_architecture_diagram():
    image_file = MY_DIR / f"{MY_FILENAME}.png"
    console.print(f"[bright_blue]Generating architecture image {image_file}")

    g = graphviz.Digraph("task_sdk_execution_architecture")
    g.attr(
        rankdir="TB",
        splines="spline",
        nodesep="0.6",
        ranksep="0.9",
        pad="0.5",
        bgcolor="white",
        fontname="Helvetica",
        newrank="true",
        compound="true",
    )
    g.attr("node", fontname="Helvetica", fontsize="13", fontcolor="#102027")
    g.attr("edge", fontname="Helvetica", fontsize="10", penwidth="1.8", color="#546e7a")

    # ------------------------------------------------------------------ #
    # Production / supervised path — several native OS processes.
    # ------------------------------------------------------------------ #
    with g.subgraph(name="cluster_prod") as prod:
        prod.attr(
            label="Production (supervised) path — one worker slot = several native OS processes",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#f7fbff",
            color="#90a4ae",
            penwidth="1.5",
            fontsize="16",
            fontname="Helvetica-Bold",
            margin="18",
        )

        with prod.subgraph(name="cluster_task") as task:
            task.attr(
                label="Task process   ·   forked native OS process (Python)   ·   runs USER CODE",
                labelloc="t",
                style="rounded,filled",
                fillcolor="#edf7ee",
                color=TASK[1],
                penwidth="1.5",
                fontsize="13",
                fontname="Helvetica-Bold",
                margin="14",
            )
            _node(
                task,
                "task_runner",
                "task_runner.run()",
                "RuntimeTaskInstance<br/>→ Operator.execute()  [user code]<br/>never sees the JWT · no DB access",
                shape="box3d",
                theme=TASK,
            )
            _node(
                task,
                "comms",
                "SUPERVISOR_COMMS",
                "CommsDecoder",
                shape="box",
                theme=TASK,
            )
            task.edge("task_runner", "comms", style="dotted", color=TASK[1], arrowhead="none")

        with prod.subgraph(name="cluster_sup") as sup:
            sup.attr(
                label="Supervisor process   ·   native OS process (Python)",
                labelloc="t",
                style="rounded,filled",
                fillcolor="#eaf3fc",
                color=SUP[1],
                penwidth="1.5",
                fontsize="13",
                fontname="Helvetica-Bold",
                margin="14",
            )
            _node(
                sup,
                "supervisor",
                "ActivitySubprocess",
                "Supervisor (WatchedSubprocess base)<br/>forks + watches the task, heartbeats,"
                "<br/>forwards logs, proxies every API call",
                shape="box3d",
                theme=SUP,
            )
            _node(
                sup,
                "client",
                "Client",
                "Execution API HTTP client<br/>holds the short-lived task JWT",
                shape="box",
                theme=SUP,
            )
            sup.edge("supervisor", "client", style="dotted", color=SUP[1], arrowhead="none")

        # Socket comms between the two native OS processes.
        g.edge(
            "comms",
            "supervisor",
            label="length-prefixed msgpack frames\nover the stdin socket\nToSupervisor ▸  ◂ ToTask",
            color=TASK[1],
            fontcolor=TASK[1],
            dir="both",
            penwidth="2.2",
        )
        g.edge(
            "comms",
            "supervisor",
            label="log records\n(line-based JSON, logs socket)",
            color=TASK[1],
            fontcolor="#5d8a60",
            style="dashed",
        )

    # ------------------------------------------------------------------ #
    # API server — separate process / host.
    # ------------------------------------------------------------------ #
    with g.subgraph(name="cluster_api") as api:
        api.attr(
            label="API server   ·   separate process / host",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#fdeeec",
            color=API[1],
            penwidth="1.5",
            fontsize="13",
            fontname="Helvetica-Bold",
            margin="14",
        )
        _node(api, "execution_api", "Execution API", "(FastAPI)", shape="component", theme=API)
        _node(api, "metadata_db", "Metadata DB", "", shape="cylinder", theme=NEUTRAL)
        api.edge("execution_api", "metadata_db", style="dotted", color=API[1], dir="both")

    g.edge(
        "client",
        "execution_api",
        label="HTTPS + task JWT\n(over the network)",
        color=API[1],
        fontcolor=API[1],
        penwidth="2.2",
    )

    # ------------------------------------------------------------------ #
    # Native / in-process path — one Python process, no fork, no sockets.
    # ------------------------------------------------------------------ #
    with g.subgraph(name="cluster_native") as nat:
        nat.attr(
            label="Native (in-process) path — dag.test() / local run"
            "   ·   ONE Python process · no fork · no sockets · no HTTP",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#faf3fc",
            color=NATIVE[1],
            penwidth="1.5",
            fontsize="16",
            fontname="Helvetica-Bold",
            margin="18",
        )
        _node(
            nat,
            "in_runner",
            "task_runner.run()",
            "→ Operator.execute()  [user code]<br/>same Task SDK runtime",
            shape="box3d",
            theme=NATIVE,
        )
        _node(
            nat,
            "in_comms",
            "InProcessSupervisorComms",
            "in-memory deques, not sockets",
            shape="box",
            theme=NATIVE,
        )
        _node(nat, "in_sup", "InProcessTestSupervisor", "(ActivitySubprocess)", shape="box3d", theme=NATIVE)
        _node(
            nat,
            "in_api",
            "InProcessExecutionAPI",
            "in-memory ASGI app · no network",
            shape="component",
            theme=NATIVE,
        )
        nat.edge(
            "in_runner",
            "in_comms",
            label="ToTask / ToSupervisor messages\nvia deque.append / popleft",
            color=NATIVE[1],
            fontcolor=NATIVE[1],
            dir="both",
        )
        nat.edge("in_comms", "in_sup", label="direct in-process call", color=NATIVE[1], fontcolor=NATIVE[1])
        nat.edge(
            "in_sup",
            "in_api",
            label="in-process ASGI request\n(no socket, no JWT)",
            color=NATIVE[1],
            fontcolor=NATIVE[1],
        )

    g.render(outfile=str(image_file), format="png", cleanup=True)
    console.print(f"[green]Generated architecture image {image_file}")


if __name__ == "__main__":
    generate_task_sdk_execution_architecture_diagram()
