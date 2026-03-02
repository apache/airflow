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
#    "diagrams>=0.23.4",
# ]
# ///
from __future__ import annotations

from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom
from diagrams.programming.flowchart import StartEnd
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name
MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name
AIRFLOW_SOURCES_ROOT = MY_DIR.parents[2]
DIAGRAMS_DIR = AIRFLOW_SOURCES_ROOT / "devel-common" / "src" / "docs" / "diagrams"
PYTHON_MULTIPROCESS_LOGO = DIAGRAMS_DIR / "python_multiprocess_logo.png"
PACKAGES_IMAGE = DIAGRAMS_DIR / "packages.png"
DATABASE_IMAGE = DIAGRAMS_DIR / "database.png"
MULTIPLE_FILES_IMAGE = DIAGRAMS_DIR / "multiple_files.png"
CONFIG_FILE = DIAGRAMS_DIR / "config_file.png"
COMPONENT_IMG = (DIAGRAMS_DIR / "task_lifecycle" / "component.png").as_posix()
CONDITION_IMG = (DIAGRAMS_DIR / "task_lifecycle" / "condition.png").as_posix()
SHARED_STATE_IMG = (DIAGRAMS_DIR / "task_lifecycle" / "shared_state.png").as_posix()
TERMINAL_STATE_IMG = (DIAGRAMS_DIR / "task_lifecycle" / "terminal_state.png").as_posix()
SENSOR_STATE_IMG = (DIAGRAMS_DIR / "task_lifecycle" / "sensor_state.png").as_posix()
DEFERRABLE_STATE_IMG = (DIAGRAMS_DIR / "task_lifecycle" / "deferrable_state.png").as_posix()

STATE_NODE_ATTRS = {"width": "4.16", "height": "1", "fontname": "Monospace", "fontsize": "20"}
COMPONENT_NODE_ATTRS = {
    "width": "3.29",
    "height": "1",
    "fontname": "Sans-Serif",
    "fontsize": "28",
    "fontcolor": "#FFFFFF",
}
CONDITION_NODE_ATTRS = {
    "width": "0.9",
    "height": "0.9",
    "labelloc": "t",
    "fontname": "Sans-Serif",
    "margin": "1.5,0.5",
}
START_NODE_ATTRS = {"width": "4", "height": "4", "fontname": "Sans-Serif", "fontsize": "28"}
LEGEND_NODE_ATTRS = {"fontname": "Sans-Serif", "fontsize": "24", "labelloc": "t"}

console = Console(width=400, color_system="standard")

graph_attr = {
    "concentrate": "false",
    "splines": "true",
}

node_attr = {
    "labelloc": "c",
}

edge_attr = {
    "minlen": "2",
    "penwidth": "4.0",
    "labelloc": "t",
    "fontsize": "14",
    "fontname": "Sans-Serif",
}


def generate_task_lifecycle_diagram():
    image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")

    console.print(f"[bright_blue]Generating task lifecycle image {image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        filename=MY_FILENAME,
        outformat="png",
        graph_attr=graph_attr,
        edge_attr=edge_attr,
        node_attr=node_attr,
    ):
        state_none = Custom("none", SHARED_STATE_IMG, **STATE_NODE_ATTRS)
        state_removed = Custom("removed", TERMINAL_STATE_IMG, **STATE_NODE_ATTRS)
        state_upstream_failed = Custom("upstream_failed", TERMINAL_STATE_IMG, **STATE_NODE_ATTRS)
        state_skipped = Custom("skipped", TERMINAL_STATE_IMG, **STATE_NODE_ATTRS)
        state_scheduled = Custom("scheduled", SHARED_STATE_IMG, **STATE_NODE_ATTRS)
        state_queued = Custom("queued", SHARED_STATE_IMG, **STATE_NODE_ATTRS)
        state_deferred = Custom("deferred", DEFERRABLE_STATE_IMG, **STATE_NODE_ATTRS)
        state_running = Custom("running", SHARED_STATE_IMG, **STATE_NODE_ATTRS)
        state_up_for_reschedule = Custom("up_for_reschedule", SENSOR_STATE_IMG, **STATE_NODE_ATTRS)
        state_restarting = Custom("restarting", SHARED_STATE_IMG, **STATE_NODE_ATTRS)
        state_up_for_retry = Custom("up_for_retry", SHARED_STATE_IMG, **STATE_NODE_ATTRS)
        state_failed = Custom("failed", TERMINAL_STATE_IMG, **STATE_NODE_ATTRS)
        state_success = Custom("success", TERMINAL_STATE_IMG, **STATE_NODE_ATTRS)

        component_scheduler = Custom("Scheduler", COMPONENT_IMG, **COMPONENT_NODE_ATTRS)
        component_executor = Custom("Executor", COMPONENT_IMG, **COMPONENT_NODE_ATTRS)
        component_triggerer = Custom("Triggerer", COMPONENT_IMG, **COMPONENT_NODE_ATTRS)
        component_worker = Custom("Worker", COMPONENT_IMG, **COMPONENT_NODE_ATTRS)

        start_node = StartEnd("Start", **START_NODE_ATTRS)

        cond_upstream_task_failure = Custom(
            "\n\n\n\n\nRequired upstream task(s) failed?", CONDITION_IMG, **CONDITION_NODE_ATTRS
        )
        cond_scheduled_skip = Custom(
            "\n\n\n\n\nTask should be skipped?", CONDITION_IMG, **CONDITION_NODE_ATTRS
        )
        cond_task_def_existence = Custom(
            "\n\n\n\n\nTask instance is still available?", CONDITION_IMG, **CONDITION_NODE_ATTRS
        )
        cond_task_restore = Custom(
            "\n\n\n\n\n                                   Task instance got restored?",
            CONDITION_IMG,
            **CONDITION_NODE_ATTRS,
        )
        cond_trigger_task_1 = Custom(
            "\n\n\n\n\nA triggerer task?\n(can execute just only with triggerer)",
            CONDITION_IMG,
            **CONDITION_NODE_ATTRS,
        )
        cond_trigger_task_2 = Custom("\n\n\n\n\nA triggerer task?", CONDITION_IMG, **CONDITION_NODE_ATTRS)
        cond_task_complete_1 = Custom(
            "\n\n\n\n\n                       Task completes?", CONDITION_IMG, **CONDITION_NODE_ATTRS
        )
        cond_task_complete_2 = Custom("\n\n\n\n\nTask completes?", CONDITION_IMG, **CONDITION_NODE_ATTRS)
        cond_defer_signal_raised = Custom(
            "\n\n\n\n\nTask is deferrable,\nand defer signal is raised?",
            CONDITION_IMG,
            **CONDITION_NODE_ATTRS,
        )
        cond_skip_signal = Custom("\n\n\n\n\nSkip signal is raised?", CONDITION_IMG, **CONDITION_NODE_ATTRS)
        cond_sensor_reschedule = Custom(
            "\n\n\n\n\nTask is a sensor in reschedule mode,\nand result is undetermined?",
            CONDITION_IMG,
            **CONDITION_NODE_ATTRS,
        )
        cond_fail_mark = Custom(
            "\n\n\n\n\nTask marked as failed?",
            CONDITION_IMG,
            **CONDITION_NODE_ATTRS,
        )
        cond_clear_mark = Custom("\n\n\n\n\nTask marked as cleared?", CONDITION_IMG, **CONDITION_NODE_ATTRS)
        cond_task_error = Custom("\n\n\n\n\nTask got error?", CONDITION_IMG, **CONDITION_NODE_ATTRS)
        cond_retriable = Custom("\n\n\n\n\nEligible for retry?", CONDITION_IMG, **CONDITION_NODE_ATTRS)

        start_node >> state_none >> component_scheduler >> cond_upstream_task_failure
        cond_upstream_task_failure >> Edge(label="NO") >> state_upstream_failed
        cond_upstream_task_failure >> Edge(label="YES") >> cond_scheduled_skip
        cond_scheduled_skip >> Edge(label="NO") >> cond_task_def_existence
        (cond_scheduled_skip >> Edge(label="YES") >> state_skipped,)
        (cond_task_def_existence >> Edge(label="NO") >> state_removed,)
        (cond_task_def_existence >> Edge(label="YES") >> state_scheduled,)
        state_removed >> cond_task_restore
        cond_task_restore >> Edge(label="NO") >> state_removed
        cond_task_restore >> Edge(label="YES") >> state_none
        state_scheduled >> component_executor >> state_queued >> cond_trigger_task_1
        cond_trigger_task_1 >> Edge(label="NO") >> component_worker
        cond_trigger_task_1 >> Edge(label="YES") >> component_triggerer
        component_triggerer >> state_deferred >> cond_trigger_task_2
        cond_trigger_task_2 >> Edge(label="NO") >> component_scheduler
        cond_trigger_task_2 >> Edge(label="YES") >> cond_task_complete_1
        cond_task_complete_1 >> Edge(label="NO") >> state_deferred
        cond_task_complete_1 >> Edge(label="YES") >> cond_defer_signal_raised
        component_worker >> state_running >> cond_defer_signal_raised
        cond_defer_signal_raised >> Edge(label="NO") >> cond_skip_signal
        cond_defer_signal_raised >> Edge(label="YES") >> component_triggerer
        cond_skip_signal >> Edge(label="NO") >> cond_sensor_reschedule
        cond_skip_signal >> Edge(label="YES") >> state_skipped
        cond_sensor_reschedule >> Edge(label="NO") >> cond_fail_mark
        cond_sensor_reschedule >> Edge(label="YES") >> state_up_for_reschedule >> component_scheduler
        cond_fail_mark >> Edge(label="NO") >> cond_clear_mark
        cond_fail_mark >> Edge(label="YES") >> state_failed
        cond_clear_mark >> Edge(label="NO") >> cond_task_error
        cond_clear_mark >> Edge(label="YES") >> state_restarting >> state_none
        cond_task_error >> Edge(label="NO") >> cond_task_complete_2
        cond_task_error >> Edge(label="YES") >> cond_retriable
        cond_retriable >> Edge(label="NO") >> state_failed
        cond_retriable >> Edge(label="YES") >> state_up_for_retry
        state_up_for_retry >> component_scheduler
        cond_task_complete_2 >> Edge(label="NO") >> state_running
        cond_task_complete_2 >> Edge(label="YES") >> state_success

        with Cluster("", graph_attr={"margin": "40,40"}):
            Custom("\n\nCondition", CONDITION_IMG, width="0.7", height="0.7", **LEGEND_NODE_ATTRS)
            Custom(
                "\n\nState for Deferrable Tasks",
                DEFERRABLE_STATE_IMG,
                width="3.2",
                height="0.77",
                **LEGEND_NODE_ATTRS,
            )
            Custom(
                "\n\nState for Sensors",
                SENSOR_STATE_IMG,
                width="3.2",
                height="0.77",
                **LEGEND_NODE_ATTRS,
            )
            Custom("\n\nTerminal State", TERMINAL_STATE_IMG, width="3.2", height="0.77", **LEGEND_NODE_ATTRS)
            Custom("\n\nShared State", SHARED_STATE_IMG, width="3.2", height="0.77", **LEGEND_NODE_ATTRS)
            Custom("\n\nComponent", COMPONENT_IMG, width="2.53", height="0.77", **LEGEND_NODE_ATTRS)

    console.print(f"[green]Generating architecture image {image_file}")


if __name__ == "__main__":
    generate_task_lifecycle_diagram()
