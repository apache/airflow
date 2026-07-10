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
Numbered-step message flow for a supervised Task SDK task run.

Graphviz cannot draw true UML sequence diagrams with lifelines, so this renders
the same information as an ordered, numbered flow read top to bottom. The task
runs down the central spine (steps 1-8); the Supervisor's HTTPS + JWT calls branch
off to the right to the Execution API (the Task never talks to it directly).

Every step names the **process** that performs it, and every arrow is coloured by
its sender:

* blue  — Supervisor ("Coordinator") → Task (both native OS processes)
* green — Task (task_runner, user code) → Supervisor
* red   — Supervisor → Execution API (FastAPI on the API server), over HTTP

Rendered with graphviz directly (rather than the ``diagrams`` library) so every
label sits *inside* a sized shape and nothing overlaps.
"""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

SUP = ("#e3f2fd", "#1565c0")  # supervisor -> task (blue)
TASK = ("#e8f5e9", "#2e7d32")  # task -> supervisor (green)
API = ("#fdecea", "#c62828")  # supervisor -> execution API (red)
ENDS = ("#fff3e0", "#ef6c00")  # start / end (amber)

SUP_C = SUP[1]
TASK_C = TASK[1]
API_C = API[1]


def _label(title: str, sub: str | None = None) -> str:
    html = f"<<b>{title}</b>"
    if sub:
        html += f'<br/><font point-size="11" color="#37474f">{sub}</font>'
    return html + ">"


def _step(g, node_id: str, title: str, sub: str, theme: tuple[str, str]) -> None:
    fill, border = theme
    g.node(
        node_id,
        label=_label(title, sub),
        shape="box",
        style="rounded,filled",
        fillcolor=fill,
        color=border,
        penwidth="2",
        margin="0.28,0.16",
    )


def _terminal(g, node_id: str, title: str, sub: str) -> None:
    fill, border = ENDS
    g.node(
        node_id,
        label=_label(title, sub),
        shape="box",
        style="rounded,filled",
        fillcolor=fill,
        color=border,
        penwidth="2",
        margin="0.3,0.14",
    )


def _api(g, node_id: str, endpoint: str) -> None:
    fill, border = API
    g.node(
        node_id,
        label=_label("Execution API", endpoint),
        shape="component",
        style="filled",
        fillcolor=fill,
        color=border,
        penwidth="2",
        margin="0.22,0.12",
    )


def generate_task_sdk_execution_sequence_diagram():
    image_file = MY_DIR / f"{MY_FILENAME}.png"
    console.print(f"[bright_blue]Generating sequence image {image_file}")

    g = graphviz.Digraph("task_sdk_execution_sequence")
    g.attr(
        rankdir="TB",
        splines="spline",
        nodesep="0.9",
        ranksep="1.25",
        pad="0.5",
        bgcolor="white",
        fontname="Helvetica",
    )
    g.attr("node", fontname="Helvetica", fontsize="13", fontcolor="#102027")
    g.attr("edge", fontname="Helvetica", fontsize="10", penwidth="2", color="#546e7a")

    # --- spine nodes (start -> 1..8 -> end) --------------------------------- #
    _terminal(g, "start", "Executor", "supervise_task() via BaseExecutor.run_workload")
    _step(
        g,
        "s1",
        "1 · Supervisor",
        "ActivitySubprocess.start()<br/>fork the task process, open the sockets",
        SUP,
    )
    _step(g, "s2", "2 · Supervisor", "tell the Execution API the TI started,<br/>receive TIRunContext", SUP)
    _step(
        g,
        "s3",
        "3 · Supervisor",
        "send StartupDetails to the task<br/>(un-prompted — the one unrequested message)",
        SUP,
    )
    _step(
        g,
        "s4",
        "4 · Task",
        "receive StartupDetails, parse() → RuntimeTaskInstance,<br/>run() → Operator.execute()  [USER CODE]",
        TASK,
    )
    _step(
        g,
        "s5",
        "5 · Task",
        "needs a Connection / Variable / XCom<br/>SUPERVISOR_COMMS.send(GetConnection / ...)",
        TASK,
    )
    _step(
        g,
        "s6",
        "6 · Supervisor",
        "proxy the request to the Execution API,<br/>return the result to the task",
        SUP,
    )
    _step(
        g,
        "s7",
        "7 · Task",
        "user code finished → send the final state<br/>SucceedTask / DeferTask / RetryTask",
        TASK,
    )
    _step(
        g,
        "s8",
        "8 · Supervisor",
        "PATCH the TI to its terminal state,<br/>upload logs, wait() → exit code",
        SUP,
    )
    _terminal(g, "end", "Task process exits", "supervise_task() returns the exit code")

    # --- Execution API nodes on the right lane ------------------------------ #
    _api(g, "api_run", "PATCH .../run → TIRunContext  (+ heartbeat every ~N s)")
    _api(g, "api_data", "GET connection / variable / xcom")
    _api(g, "api_state", "PATCH .../state")

    # Keep the three API nodes aligned to the right of their originating steps.
    for step, api_node in (("s2", "api_run"), ("s6", "api_data"), ("s8", "api_state")):
        with g.subgraph() as same:
            same.attr(rank="same")
            same.node(step)
            same.node(api_node)
    # Invisible column so the API nodes stack neatly on the right.
    g.edge("api_run", "api_data", style="invis")
    g.edge("api_data", "api_state", style="invis")

    # --- spine edges (colour = sender) -------------------------------------- #
    g.edge("start", "s1", color=SUP_C)
    g.edge("s1", "s2", color=SUP_C, fontcolor=SUP_C, label="fork() + exec →\nnew native OS process")
    g.edge("s2", "s3", color=SUP_C)
    g.edge(
        "s3",
        "s4",
        color=SUP_C,
        fontcolor=SUP_C,
        label="ToTask: StartupDetails\n(_ResponseFrame, msgpack)",
    )
    g.edge("s4", "s5", color=TASK_C, fontcolor=TASK_C, label="user code needs\nexternal data")
    g.edge(
        "s5",
        "s6",
        color=TASK_C,
        fontcolor=TASK_C,
        label="ToSupervisor request\nGetConnection / GetVariable / GetXCom\n(_RequestFrame, msgpack)",
    )
    g.edge(
        "s6",
        "s7",
        color=SUP_C,
        fontcolor=SUP_C,
        label="ToTask response: *Result →\ntask resumes (steps 5–6 repeat\nper lookup), then completes",
    )
    g.edge(
        "s7", "s8", color=TASK_C, fontcolor=TASK_C, label="ToSupervisor: final state\n(msgpack over socket)"
    )
    g.edge("s8", "end", color=SUP_C)

    # --- Supervisor -> Execution API calls (red, dotted, short & horizontal) - #
    g.edge("s2", "api_run", color=API_C, style="dotted", constraint="false")
    g.edge("s6", "api_data", color=API_C, style="dotted", constraint="false")
    g.edge("s8", "api_state", color=API_C, style="dotted", constraint="false")

    # --- legend ------------------------------------------------------------- #
    with g.subgraph(name="cluster_legend") as legend:
        legend.attr(
            label="Legend",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#fafafa",
            color="#b0bec5",
            fontsize="12",
            fontname="Helvetica-Bold",
            margin="10",
        )
        legend.node(
            "lg_sup",
            label=_label("Supervisor → Task"),
            shape="box",
            style="rounded,filled",
            fillcolor=SUP[0],
            color=SUP[1],
            penwidth="2",
            margin="0.2,0.1",
        )
        legend.node(
            "lg_task",
            label=_label("Task → Supervisor"),
            shape="box",
            style="rounded,filled",
            fillcolor=TASK[0],
            color=TASK[1],
            penwidth="2",
            margin="0.2,0.1",
        )
        legend.node(
            "lg_api",
            label=_label("Execution API (FastAPI)"),
            shape="component",
            style="filled",
            fillcolor=API[0],
            color=API[1],
            penwidth="2",
            margin="0.2,0.1",
        )
        legend.edge("lg_sup", "lg_task", color=SUP_C, label="msgpack over socket", fontcolor="#546e7a")
        legend.edge("lg_task", "lg_api", color=API_C, style="dotted", label="HTTPS + JWT", fontcolor=API_C)

    g.render(outfile=str(image_file), format="png", cleanup=True)
    console.print(f"[green]Generated sequence image {image_file}")


if __name__ == "__main__":
    generate_task_sdk_execution_sequence_diagram()
