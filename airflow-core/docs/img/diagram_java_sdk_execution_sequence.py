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
Numbered execution workflow for a Java (JVM) task, read top to bottom.

From a ``@task.stub(queue="java")`` in a Python Dag through to the JVM subprocess
running the user's Java/Kotlin code and back. The task runs down the central spine
(steps 1-10); the Supervisor's HTTPS + JWT calls branch to the Execution API on the
right (the JVM task never talks to it directly — the Supervisor proxies for it).

Each step names the component that performs it; arrows are coloured by sender:

* teal   — Scheduler
* purple — Coordinator layer (CoordinatorManager / JavaCoordinator)
* blue   — Supervisor (_JavaActivitySubprocess) → JVM, over loopback TCP
* orange — JVM SDK runtime → Supervisor, over loopback TCP
* green  — user Java/Kotlin task code
* red    — Supervisor → Execution API, over HTTP

Rendered with graphviz directly so every label sits inside a sized shape.
"""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

SCHED = ("#e0f2f1", "#00695c")  # scheduler (teal)
COORD = ("#ede7f6", "#5e35b1")  # coordinator layer (purple)
SUP = ("#e3f2fd", "#1565c0")  # supervisor (blue)
JVM = ("#fbe9e7", "#d84315")  # JVM runtime (orange)
USER = ("#e8f5e9", "#2e7d32")  # user task code (green)
API = ("#fdecea", "#c62828")  # execution API (red)
ENDS = ("#fff3e0", "#ef6c00")  # start / end (amber)

SCHED_C, COORD_C, SUP_C, JVM_C, API_C = SCHED[1], COORD[1], SUP[1], JVM[1], API[1]


def _label(title: str, sub: str | None = None) -> str:
    html = f"<<b>{title}</b>"
    if sub:
        html += f'<br/><font point-size="11" color="#37474f">{sub}</font>'
    return html + ">"


def _step(g, node_id: str, title: str, sub: str, theme: tuple[str, str], shape: str = "box") -> None:
    fill, border = theme
    style = "filled" if shape in ("component", "cylinder") else "rounded,filled"
    g.node(
        node_id,
        label=_label(title, sub),
        shape=shape,
        style=style,
        fillcolor=fill,
        color=border,
        penwidth="2",
        margin="0.28,0.16",
    )


def generate_java_sdk_execution_sequence_diagram():
    image_file = MY_DIR / f"{MY_FILENAME}.png"
    console.print(f"[bright_blue]Generating sequence image {image_file}")

    g = graphviz.Digraph("java_sdk_execution_sequence")
    g.attr(
        rankdir="TB",
        splines="spline",
        nodesep="1.1",
        ranksep="1.25",
        pad="0.5",
        bgcolor="white",
        fontname="Helvetica",
    )
    g.attr("node", fontname="Helvetica", fontsize="13", fontcolor="#102027")
    g.attr("edge", fontname="Helvetica", fontsize="10", penwidth="2", color="#546e7a")

    # --- spine nodes -------------------------------------------------------- #
    _step(g, "start", "Dag author", '@task.stub(queue="java") in a Python Dag', ENDS)
    _step(
        g,
        "s1",
        "1 · Scheduler",
        "parse Dag (standard Python) → TI.queue = &quot;java&quot;<br/>emit ExecuteTask workload (carries the queue)",
        SCHED,
    )
    _step(
        g,
        "s2",
        "2 · Supervisor",
        'CoordinatorManager.for_queue("java")<br/>→ JavaCoordinator (via [sdk] queue_to_coordinator)',
        COORD,
    )
    _step(
        g,
        "s3",
        "3 · JavaCoordinator",
        "execute_task(client): open two loopback-TCP servers,<br/>subprocess.Popen(java -jar bundle)",
        COORD,
    )
    _step(
        g,
        "s4",
        "4 · JVM",
        "Server.serve(bundle) starts and<br/>connects back to the TCP sockets",
        JVM,
    )
    _step(
        g,
        "s5",
        "5 · Supervisor",
        "send StartupDetails (msgpack over TCP) → build Context<br/>mark the TI running on the Execution API",
        SUP,
    )
    _step(
        g,
        "s6",
        "6 · JVM",
        "run Task.execute(Context, Client)  [USER CODE]",
        USER,
    )
    _step(
        g,
        "s7",
        "7 · JVM",
        "Client.getConnection / getVariable / getXCom / setXCom<br/>→ msgpack request over loopback TCP",
        JVM,
    )
    _step(
        g,
        "s8",
        "8 · Supervisor",
        "proxy the request to the Execution API,<br/>return the result to the JVM over TCP",
        SUP,
    )
    _step(
        g,
        "s9",
        "9 · JVM",
        "user code finished → send the final state<br/>(success / failed / up-for-retry) over TCP",
        JVM,
    )
    _step(
        g,
        "s10",
        "10 · Supervisor",
        "PATCH the TI to its terminal state,<br/>upload logs, wait() → exit code",
        SUP,
    )
    _step(g, "end", "JVM process exits", "the coordinator's execute_task() returns the exit code", ENDS)

    # --- Execution API lifeline on the right -------------------------------- #
    _step(
        g, "api_run", "Execution API", "PATCH .../run → TIRunContext  (+ heartbeat)", API, shape="component"
    )
    _step(g, "api_data", "Execution API", "GET connection / variable / xcom", API, shape="component")
    _step(g, "api_state", "Execution API", "PATCH .../state", API, shape="component")

    for step, api_node in (("s5", "api_run"), ("s8", "api_data"), ("s10", "api_state")):
        with g.subgraph() as same:
            same.attr(rank="same")
            same.node(step)
            same.node(api_node)
    g.edge("api_run", "api_data", style="invis")
    g.edge("api_data", "api_state", style="invis")

    # --- spine edges (colour = sender) -------------------------------------- #
    g.edge("start", "s1", color=SCHED_C)
    g.edge("s1", "s2", color=SCHED_C, fontcolor=SCHED_C, label="ExecuteTask workload\n(includes queue)")
    g.edge("s2", "s3", color=COORD_C)
    g.edge("s3", "s4", color=COORD_C, fontcolor=COORD_C, label="subprocess.Popen\n(spawn JVM)")
    g.edge("s4", "s5", color=JVM_C, fontcolor=JVM_C, label="TCP connect back\n(comm + logs)")
    g.edge("s5", "s6", color=SUP_C, fontcolor=SUP_C, label="ToTask: StartupDetails\n(msgpack over TCP)")
    g.edge("s6", "s7", color=USER[1], fontcolor=USER[1], label="user code needs\nexternal data")
    g.edge(
        "s7",
        "s8",
        color=JVM_C,
        fontcolor=JVM_C,
        label="ToSupervisor request\n(_RequestFrame, msgpack over TCP)",
    )
    g.edge(
        "s8",
        "s9",
        color=SUP_C,
        fontcolor=SUP_C,
        label="ToTask response: *Result →\nJVM resumes (steps 7–8 repeat\nper lookup), then completes",
    )
    g.edge("s9", "s10", color=JVM_C, fontcolor=JVM_C, label="ToSupervisor: final state\n(msgpack over TCP)")
    g.edge("s10", "end", color=SUP_C)

    # --- Supervisor -> Execution API calls (red, dotted, short & horizontal) - #
    g.edge("s5", "api_run", color=API_C, style="dotted", constraint="false")
    g.edge("s8", "api_data", color=API_C, style="dotted", constraint="false")
    g.edge("s10", "api_state", color=API_C, style="dotted", constraint="false")

    # --- legend ------------------------------------------------------------- #
    with g.subgraph(name="cluster_legend") as legend:
        legend.attr(
            label="Arrow colour = sender",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#fafafa",
            color="#b0bec5",
            fontsize="12",
            fontname="Helvetica-Bold",
            margin="10",
        )
        for nid, text, theme in (
            ("lg_sched", "Scheduler", SCHED),
            ("lg_coord", "Coordinator layer", COORD),
            ("lg_sup", "Supervisor → JVM (TCP)", SUP),
            ("lg_jvm", "JVM → Supervisor (TCP)", JVM),
            ("lg_api", "Supervisor → Execution API", API),
        ):
            legend.node(
                nid,
                label=_label(text),
                shape="box",
                style="rounded,filled",
                fillcolor=theme[0],
                color=theme[1],
                penwidth="2",
                margin="0.2,0.1",
            )
        legend.edge("lg_sched", "lg_coord", style="invis")
        legend.edge("lg_coord", "lg_sup", style="invis")
        legend.edge("lg_sup", "lg_jvm", style="invis")
        legend.edge("lg_jvm", "lg_api", style="invis")

    g.render(outfile=str(image_file), format="png", cleanup=True)
    console.print(f"[green]Generated sequence image {image_file}")


if __name__ == "__main__":
    generate_java_sdk_execution_sequence_diagram()
