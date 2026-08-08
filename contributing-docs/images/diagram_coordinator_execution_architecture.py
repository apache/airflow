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
Architecture diagram for the Coordinator execution path (Java and Go language SDKs).

Both plug into the *same* Python Supervisor via the **Coordinator** layer, rather
than running a standalone worker:

* ``CoordinatorManager.for_queue()`` resolves the task's ``queue`` to a
  ``BaseCoordinator`` via ``[sdk] queue_to_coordinator`` / ``[sdk] coordinators``
  (``_PythonCoordinator`` for unmapped queues);
* the resolved ``SubprocessCoordinator`` (``JavaCoordinator`` for Java,
  ``ExecutableCoordinator`` for Go) binds two loopback-TCP servers, builds the
  per-language launch command, spawns the bundle subprocess with ``--comm`` /
  ``--logs`` appended, and drives it through ``_PopenActivitySubprocess`` — a
  subclass of the shared ``ActivitySubprocess``;
* the language subprocess connects *back* over TCP and speaks the same msgpack
  protocol as a Python task, so the Python side heartbeats, proxies every
  Execution-API call, and manages state. The language task therefore **never
  holds the task JWT**.

The launch command is the only per-language difference: Java runs
``java -classpath ... <main class>``; Go execs the self-contained packed bundle
binary directly.

Rendered with graphviz directly so labels sit inside sized shapes: 3-D box =
native OS process, rounded box = an object inside a process, component = a server
app, cylinder = the database, note = a caption.
"""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

# (fill, border) per role — consistent with the other Task SDK diagrams.
COORD = ("#ede7f6", "#5e35b1")  # Coordinator layer (Python, deep purple)
SUP = ("#e3f2fd", "#1565c0")  # Supervisor / ActivitySubprocess + Client (blue)
LANG = ("#fbe9e7", "#d84315")  # language SDK runtime — JVM or compiled binary (deep orange)
USER = ("#e8f5e9", "#2e7d32")  # user task code (green)
API = ("#fdecea", "#c62828")  # Execution API (red)
NEUTRAL = ("#eceff1", "#455a64")  # database
NOTE = ("#fffde7", "#f9a825")  # caption


def _label(title: str, sub: str | None = None) -> str:
    html = f"<<b>{title}</b>"
    if sub:
        html += f'<br/><font point-size="11" color="#37474f">{sub}</font>'
    return html + ">"


def _node(g, node_id: str, title: str, sub: str, *, shape: str, theme: tuple[str, str]) -> None:
    fill, border = theme
    style = "filled" if shape in ("box3d", "cylinder", "component", "note") else "rounded,filled"
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


def generate_coordinator_execution_architecture_diagram():
    image_file = MY_DIR / f"{MY_FILENAME}.png"
    console.print(f"[bright_blue]Generating architecture image {image_file}")

    g = graphviz.Digraph("coordinator_execution_architecture")
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
    # Worker host — native OS processes.
    # ------------------------------------------------------------------ #
    with g.subgraph(name="cluster_host") as host:
        host.attr(
            label="Worker host — native OS processes",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#f7f7fb",
            color="#90a4ae",
            penwidth="1.5",
            fontsize="16",
            fontname="Helvetica-Bold",
            margin="18",
        )

        with host.subgraph(name="cluster_sup") as sup:
            sup.attr(
                label="Supervisor process   ·   native OS process (Python)",
                labelloc="t",
                style="rounded,filled",
                fillcolor="#eef0fb",
                color=SUP[1],
                penwidth="1.5",
                fontsize="13",
                fontname="Helvetica-Bold",
                margin="14",
            )
            _node(
                sup,
                "coord_mgr",
                "CoordinatorManager.for_queue()",
                "resolves the task queue → a coordinator via<br/>[sdk] queue_to_coordinator / [sdk] coordinators",
                shape="box",
                theme=COORD,
            )
            _node(
                sup,
                "coord",
                "SubprocessCoordinator",
                "JavaCoordinator (java) · ExecutableCoordinator (golang)<br/>"
                "bind two loopback-TCP servers · spawn bundle subprocess<br/>"
                "with --comm / --logs appended",
                shape="box",
                theme=COORD,
            )
            _node(
                sup,
                "supervisor",
                "_PopenActivitySubprocess",
                "subclass of the shared ActivitySubprocess<br/>heartbeat · proxy every API call · manage state",
                shape="box3d",
                theme=SUP,
            )
            _node(
                sup,
                "client",
                "Client",
                "authenticated Execution API client<br/>holds the short-lived task JWT",
                shape="box",
                theme=SUP,
            )
            sup.edge("coord_mgr", "coord", style="dotted", color=COORD[1], arrowhead="vee", label="picks")
            sup.edge("coord", "supervisor", style="dotted", color=SUP[1], arrowhead="vee", label="drives")
            sup.edge("supervisor", "client", style="dotted", color=SUP[1], arrowhead="none")

        with host.subgraph(name="cluster_lang") as lang:
            lang.attr(
                label="Language bundle subprocess   ·   native OS process (JVM or compiled binary)   ·   runs USER CODE",
                labelloc="t",
                style="rounded,filled",
                fillcolor="#fdefe9",
                color=LANG[1],
                penwidth="1.5",
                fontsize="13",
                fontname="Helvetica-Bold",
                margin="14",
            )
            _node(
                lang,
                "server",
                "bundle entry point",
                "Java: java -classpath … main class (JVM)<br/>"
                "Go: exec the packed bundle binary (compiled)<br/>"
                "connects back over TCP",
                shape="box3d",
                theme=LANG,
            )
            _node(
                lang,
                "task",
                "task function   [user code]",
                "your Java / Kotlin or Go task<br/>Context = run data · Client = API accessors",
                shape="box3d",
                theme=USER,
            )
            _node(
                lang,
                "comm",
                "CoordinatorComm / Frame",
                "length-prefixed msgpack-over-IPC framing",
                shape="box",
                theme=LANG,
            )
            lang.edge("server", "task", style="dotted", color=LANG[1], arrowhead="vee", label="invokes")
            lang.edge("task", "comm", style="dotted", color=LANG[1], arrowhead="none")

        # Loopback-TCP comms between the two native OS processes (the subprocess connects back).
        g.edge(
            "comm",
            "supervisor",
            label="msgpack frames over loopback TCP (127.0.0.1)\nsubprocess connects back · ToSupervisor / ToTask",
            color=LANG[1],
            fontcolor=LANG[1],
            dir="both",
            penwidth="2.2",
        )
        g.edge(
            "comm",
            "supervisor",
            label="structured logs\n(second TCP channel)",
            color=LANG[1],
            fontcolor="#a1674f",
            style="dashed",
        )

    # ------------------------------------------------------------------ #
    # API server — separate process / host.
    # ------------------------------------------------------------------ #
    with g.subgraph(name="cluster_server") as server:
        server.attr(
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
        _node(
            server,
            "execution_api",
            "Execution API",
            "FastAPI · TEI / AIP-72",
            shape="component",
            theme=API,
        )
        _node(server, "metadata_db", "Metadata DB", "", shape="cylinder", theme=NEUTRAL)
        server.edge("execution_api", "metadata_db", style="dotted", color=API[1], dir="both")

    g.edge(
        "client",
        "execution_api",
        label="HTTPS + task JWT\n(proxied for the language task)",
        color=API[1],
        fontcolor=API[1],
        penwidth="2.2",
    )

    # Contrast caption.
    _node(
        g,
        "note",
        "How it differs",
        "vs Python task: same Supervisor, but a separate language subprocess over<br/>"
        "loopback TCP instead of a forked Python process over a UNIX socketpair<br/>"
        "vs the Go edge worker: the coordinator task does <b>not</b> hold the JWT and does<br/>"
        "<b>not</b> call the Execution API directly — the Python Supervisor proxies every call",
        shape="note",
        theme=NOTE,
    )
    g.edge("supervisor", "note", style="invis")

    g.render(outfile=str(image_file), format="png", cleanup=True)
    console.print(f"[green]Generated architecture image {image_file}")


if __name__ == "__main__":
    generate_coordinator_execution_architecture_diagram()
