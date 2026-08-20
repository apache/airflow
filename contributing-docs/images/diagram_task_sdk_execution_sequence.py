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
UML-style sequence diagram for a supervised Task SDK task run.

Each participant gets its own vertical lifeline; messages are horizontal arrows
between lifelines, read top to bottom. The **Supervisor** sits in the middle so
the Task <-> Supervisor round-trip (the task asks for a Connection/Variable/XCom
and gets the answer back) and the Supervisor <-> Execution API round-trip are
both drawn as adjacent request/response pairs.

Arrows are colored by sender:

* blue   — Supervisor ("Coordinator")  → Task / Execution API
* green  — Task (task_runner, user code)  → Supervisor
* red    — Execution API (FastAPI on the API server)  → Supervisor  (responses)
* amber  — Executor (start / end of the run)

Graphviz has no native sequence-diagram shape, so lifelines are drawn as dashed
vertical edges through invisible way-points, one row per message, with each
message as a ``constraint=false`` horizontal edge on that row.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

# (fill, border) per participant — consistent with the other Task SDK diagrams.
EXEC = ("#fff3e0", "#ef6c00")  # executor / terminal (amber)
TASK = ("#e8f5e9", "#2e7d32")  # task_runner + user code (green)
SUP = ("#e3f2fd", "#1565c0")  # supervisor (blue)
API = ("#fdecea", "#c62828")  # execution API (red)

EXEC_C, TASK_C, SUP_C, API_C = EXEC[1], TASK[1], SUP[1], API[1]

LIFELINE = "#b0bec5"

# Participants, left to right. Supervisor is central so both round-trips are
# drawn between neighboring lifelines.
PARTICIPANTS = [
    ("exec", "Executor", "supervise_task()", EXEC),
    ("task", "Task", "task_runner · user code", TASK),
    ("sup", "Supervisor", "ActivitySubprocess", SUP),
    ("api", "Execution API", "FastAPI · TEI / AIP-72", API),
]

# Each step is one row. A "msg" is an arrow between two lifelines; a "self" is an
# activation box on a single lifeline (local processing, no message).
STEPS: list[dict[str, Any]] = [
    {
        "kind": "msg",
        "from": "exec",
        "to": "sup",
        "color": EXEC_C,
        "label": "supervise_task()\nvia BaseExecutor.run_workload",
    },
    {
        "kind": "self",
        "actor": "sup",
        "theme": SUP,
        "text": "ActivitySubprocess.start()\nfork the task process, open the sockets",
    },
    {"kind": "msg", "from": "sup", "to": "api", "color": SUP_C, "label": "PATCH .../run\n(TI started)"},
    {
        "kind": "msg",
        "from": "api",
        "to": "sup",
        "color": API_C,
        "style": "dashed",
        "label": "TIRunContext\n(+ heartbeat every ~N s)",
    },
    {
        "kind": "msg",
        "from": "sup",
        "to": "task",
        "color": SUP_C,
        "label": "StartupDetails\n(_ResponseFrame, msgpack —\nthe one un-prompted message)",
    },
    {
        "kind": "self",
        "actor": "task",
        "theme": TASK,
        "text": "parse() → RuntimeTaskInstance\nrun() → Operator.execute()  [USER CODE]",
    },
    {
        "kind": "msg",
        "from": "task",
        "to": "sup",
        "color": TASK_C,
        "label": "GetConnection / GetVariable / GetXCom\n(_RequestFrame, msgpack over socket)",
    },
    {
        "kind": "msg",
        "from": "sup",
        "to": "api",
        "color": SUP_C,
        "label": "GET connection / variable / xcom\n(HTTPS + task JWT)",
    },
    {"kind": "msg", "from": "api", "to": "sup", "color": API_C, "style": "dashed", "label": "result"},
    {
        "kind": "msg",
        "from": "sup",
        "to": "task",
        "color": SUP_C,
        "label": "*Result (_ResponseFrame, msgpack)\nabove 4 messages repeat per lookup",
    },
    {
        "kind": "msg",
        "from": "task",
        "to": "sup",
        "color": TASK_C,
        "label": "SucceedTask / DeferTask / RetryTask\n(final state, msgpack over socket)",
    },
    {"kind": "msg", "from": "sup", "to": "api", "color": SUP_C, "label": "PATCH .../state\n(+ upload logs)"},
    {
        "kind": "msg",
        "from": "sup",
        "to": "exec",
        "color": EXEC_C,
        "style": "dashed",
        "label": "task process exits →\nsupervise_task() returns the exit code",
    },
]


def _label(title: str, sub: str | None = None) -> str:
    html = f"<<b>{title}</b>"
    if sub:
        safe = sub.replace("\n", "<br/>")
        html += f'<br/><font point-size="11" color="#37474f">{safe}</font>'
    return html + ">"


def _header(g, pid: str, title: str, sub: str, theme: tuple[str, str]) -> None:
    fill, border = theme
    g.node(
        f"{pid}__h",
        label=_label(title, sub),
        shape="box",
        style="rounded,filled",
        fillcolor=fill,
        color=border,
        penwidth="2",
        margin="0.24,0.14",
    )


def _activation(g, node_id: str, num: int, text: str, theme: tuple[str, str]) -> None:
    fill, border = theme
    g.node(
        node_id,
        label=_label(f"{num}", text),
        shape="box",
        style="rounded,filled",
        fillcolor=fill,
        color=border,
        penwidth="2",
        margin="0.2,0.12",
    )


def _waypoint(g, node_id: str) -> None:
    g.node(node_id, shape="point", width="0.02", color=LIFELINE)


def _edge_label(num: int, text: str) -> str:
    # Pad the description on every side: blank lines above and below keep it clear
    # of the arrow line, and leading/trailing spaces on each line keep it clear of
    # the lifelines on the left and right.
    lines = f"{num} · {text}".split("\n")
    padded = "\n".join(f"   {line}   " for line in lines)
    return f" \n{padded}\n "


def _build_sequence(g) -> None:
    ids = [p[0] for p in PARTICIPANTS]

    # --- participant headers, ordered left-to-right on the top rank ---------- #
    with g.subgraph() as top:
        top.attr(rank="same")
        for pid, title, sub, theme in PARTICIPANTS:
            _header(top, pid, title, sub, theme)
    for a, b in zip(ids, ids[1:]):
        g.edge(f"{a}__h", f"{b}__h", style="invis")

    # --- one rank per step; way-points on every lifeline, box on the actor --- #
    for i, step in enumerate(STEPS):
        with g.subgraph() as row:
            row.attr(rank="same")
            for pid, _, _, theme in PARTICIPANTS:
                nid = f"{pid}__{i}"
                if step["kind"] == "self" and step["actor"] == pid:
                    _activation(row, nid, i + 1, step["text"], step.get("theme", theme))
                else:
                    _waypoint(row, nid)

    # --- dashed vertical lifelines through the way-points -------------------- #
    for pid, *_ in PARTICIPANTS:
        chain = [f"{pid}__h"] + [f"{pid}__{i}" for i in range(len(STEPS))]
        for a, b in zip(chain, chain[1:]):
            g.edge(a, b, style="dashed", arrowhead="none", color=LIFELINE, penwidth="1.3")

    # --- message arrows (horizontal, do not constrain ranking) --------------- #
    for i, step in enumerate(STEPS):
        if step["kind"] != "msg":
            continue
        g.edge(
            f"{step['from']}__{i}",
            f"{step['to']}__{i}",
            xlabel=_edge_label(i + 1, step["label"]),
            color=step["color"],
            fontcolor=step["color"],
            style=step.get("style", "solid"),
            arrowhead="vee",
            penwidth="2",
            constraint="false",
        )

    _legend(g)


def _legend(g) -> None:
    entries = [
        ("lg_sup", "Supervisor  →  Task / Execution API", SUP),
        ("lg_task", "Task (user code)  →  Supervisor", TASK),
        ("lg_api", "Execution API  →  Supervisor  (response)", API),
        ("lg_exec", "Executor  (start / end of the run)", EXEC),
    ]
    with g.subgraph(name="cluster_legend") as legend:
        legend.attr(
            label="Arrow color = sender  ·  steps 7–10 repeat per Connection / Variable / XCom lookup",
            labelloc="t",
            style="rounded,filled",
            fillcolor="#fafafa",
            color="#b0bec5",
            fontsize="12",
            fontname="Helvetica-Bold",
            margin="12",
        )
        for nid, text, theme in entries:
            fill, border = theme
            legend.node(
                nid,
                label=_label(text),
                shape="box",
                style="rounded,filled",
                fillcolor=fill,
                color=border,
                penwidth="2",
                margin="0.2,0.1",
            )
        for a, b in zip([e[0] for e in entries], [e[0] for e in entries][1:]):
            legend.edge(a, b, style="invis")
    # Anchor the legend below the diagram, on the left.
    g.edge(f"exec__{len(STEPS) - 1}", "lg_sup", style="invis")


def generate_task_sdk_execution_sequence_diagram():
    image_file = MY_DIR / f"{MY_FILENAME}.png"
    console.print(f"[bright_blue]Generating sequence image {image_file}")

    g = graphviz.Digraph("task_sdk_execution_sequence")
    g.attr(
        rankdir="TB",
        splines="line",
        forcelabels="true",
        nodesep="1.3",
        ranksep="1.2",
        pad="0.5",
        bgcolor="white",
        fontname="Helvetica",
    )
    g.attr("node", fontname="Helvetica", fontsize="13", fontcolor="#102027")
    g.attr("edge", fontname="Helvetica", fontsize="10", penwidth="2", color="#546e7a")

    _build_sequence(g)

    g.render(outfile=str(image_file), format="png", cleanup=True)
    console.print(f"[green]Generated sequence image {image_file}")


if __name__ == "__main__":
    generate_task_sdk_execution_sequence_diagram()
