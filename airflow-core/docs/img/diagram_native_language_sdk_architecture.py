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
Architecture diagram for a native-language (compiled) Task SDK.

The Go SDK (``go-sdk/``) is the first implementation; the same shape applies to
any future compiled-language SDK (e.g. Java). Unlike the Python Task SDK there is
**no Python Supervisor and no msgpack stdin socket**:

* a long-running compiled **edge worker** (``airflow-go-edge-worker``) *pulls*
  work from the **Edge Executor API** over HTTP;
* it launches the user's compiled **Dag bundle** as a **go-plugin (gRPC)
  subprocess** and invokes the task over gRPC; and
* the task uses the **native Task Execution Interface client** (AIP-72) to reach
  the **Execution API** directly over HTTPS — so, unlike the Python task, it holds
  the task JWT itself.

Rendered with graphviz directly so every label sits inside a sized shape:
3-D box = native OS process, rounded box = an object inside a process,
component = a server app, cylinder = the database, note = a caption.
"""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

# Soft palette: (fill, border) per role. Kept consistent with the Python Task SDK
# diagrams: user code = green, Execution API = red, DB = grey.
GO = ("#e0f7fa", "#00838f")  # compiled edge worker / native clients (Go cyan)
USER = ("#e8f5e9", "#2e7d32")  # user task code
EDGE = ("#fff3e0", "#ef6c00")  # Edge Executor API (amber)
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


def generate_native_language_sdk_architecture_diagram():
    image_file = MY_DIR / f"{MY_FILENAME}.png"
    console.print(f"[bright_blue]Generating architecture image {image_file}")

    g = graphviz.Digraph("native_language_sdk_architecture")
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
    # Worker host — compiled native OS processes, no Python runtime.
    # ------------------------------------------------------------------ #
    with g.subgraph(name="cluster_host") as host:
        host.attr(
            label="Worker host — native OS processes (compiled binary · no Python runtime)",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#f4fdff",
            color="#90a4ae",
            penwidth="1.5",
            fontsize="16",
            fontname="Helvetica-Bold",
            margin="18",
        )

        with host.subgraph(name="cluster_worker") as worker:
            worker.attr(
                label="Edge-worker process   ·   long-running compiled binary",
                labelloc="t",
                style="rounded,filled",
                fillcolor="#e7fbff",
                color=GO[1],
                penwidth="1.5",
                fontsize="13",
                fontname="Helvetica-Bold",
                margin="14",
            )
            _node(
                worker,
                "worker",
                "airflow-go-edge-worker",
                "register · heartbeat · fetch workloads<br/>report task state · spawn bundle plugins",
                shape="box3d",
                theme=GO,
            )
            _node(
                worker,
                "edge_client",
                "edgeapi.Client",
                "Edge Executor API client",
                shape="box",
                theme=GO,
            )
            worker.edge("worker", "edge_client", style="dotted", color=GO[1], arrowhead="none")

        with host.subgraph(name="cluster_bundle") as bundle:
            bundle.attr(
                label="Dag bundle plugin process   ·   go-plugin (gRPC) subprocess   ·   runs USER CODE",
                labelloc="t",
                style="rounded,filled",
                fillcolor="#edf7ee",
                color=USER[1],
                penwidth="1.5",
                fontsize="13",
                fontname="Helvetica-Bold",
                margin="14",
            )
            _node(
                bundle,
                "gotask",
                "Go task function",
                "your compiled task code, registered in the bundle<br/>holds the task JWT",
                shape="box3d",
                theme=USER,
            )
            _node(
                bundle,
                "tei_client",
                "sdk client → pkg/api",
                "native Task Execution Interface client (AIP-72)<br/>"
                "VariableClient · ConnectionClient · XComClient",
                shape="box",
                theme=GO,
            )
            bundle.edge("gotask", "tei_client", style="dotted", color=GO[1], arrowhead="none")

        # Worker launches the bundle plugin and invokes the task over gRPC.
        g.edge(
            "worker",
            "gotask",
            label="go-plugin gRPC\nExecute(ExecuteTaskWorkload)",
            color=GO[1],
            fontcolor=GO[1],
            penwidth="2.2",
        )

    # ------------------------------------------------------------------ #
    # Airflow API server — separate process / host.
    # ------------------------------------------------------------------ #
    with g.subgraph(name="cluster_server") as server:
        server.attr(
            label="Airflow API server   ·   separate process / host",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#fdf6ec",
            color="#b0846a",
            penwidth="1.5",
            fontsize="14",
            fontname="Helvetica-Bold",
            margin="16",
        )
        _node(server, "edge_api", "Edge Executor API", "(edge provider)", shape="component", theme=EDGE)
        _node(
            server,
            "execution_api",
            "Execution API",
            "FastAPI · TEI / AIP-72",
            shape="component",
            theme=API,
        )
        _node(server, "metadata_db", "Metadata DB", "", shape="cylinder", theme=NEUTRAL)
        server.edge("edge_api", "metadata_db", style="dotted", color=EDGE[1], dir="both")
        server.edge("execution_api", "metadata_db", style="dotted", color=API[1], dir="both")

    g.edge(
        "edge_client",
        "edge_api",
        label="HTTP + Edge API JWT\nregister · heartbeat · poll for\nworkloads · report state",
        color=EDGE[1],
        fontcolor=EDGE[1],
        penwidth="2.2",
    )
    g.edge(
        "tei_client",
        "execution_api",
        label="HTTPS + task JWT\nVariables · Connections · XComs · state",
        color=API[1],
        fontcolor=API[1],
        penwidth="2.2",
    )

    # Contrast caption.
    _node(
        g,
        "note",
        "Contrast with the Python Task SDK",
        "no Python Supervisor · no msgpack stdin socket<br/>"
        "work is <b>pulled</b> via the Edge API (not pushed by an executor)<br/>"
        "the task runs as a compiled gRPC plugin and calls the<br/>"
        "Execution API directly — so it holds the JWT itself",
        shape="note",
        theme=NOTE,
    )
    g.edge("worker", "note", style="invis")

    g.render(outfile=str(image_file), format="png", cleanup=True)
    console.print(f"[green]Generated architecture image {image_file}")


if __name__ == "__main__":
    generate_native_language_sdk_architecture_diagram()
