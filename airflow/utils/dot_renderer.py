#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
"""
Renderer DAG (tasks and dependencies) to the graphviz object.
"""

import graphviz


def _refine_color(color):
    """
    Converts color in #RGB (12 bits) format to #RRGGBB (32 bits), if it possible.
    Otherwise, it returns the original value. Graphviz does not support colors in #RGB format.

    :param color: Text representation of color
    :return: Refined representation of color
    """
    if len(color) == 4 and color[0] == "#":
        color_r = color[1]
        color_g = color[2]
        color_b = color[3]
        return "#" + color_r + color_r + color_g + color_g + color_b + color_b
    return color


def render_dag(dag):
    """
    Renders the DAG object to the DOT object.

    :param dag: DAG that will be rendered.
    :return: Graphviz object
    :rtype: graphviz.Digraph
    """
    dot = graphviz.Digraph(dag.dag_id, graph_attr={"rankdir": "LR", "labelloc": "t", "label": dag.dag_id})
    for task in dag.tasks:
        dot.node(
            task.task_id,
            _attributes={
                "shape": "rectangle",
                "style": "filled,rounded",
                "color": _refine_color(task.ui_fgcolor),
                "fillcolor": _refine_color(task.ui_color),
            },
        )
        for downstream_task_id in task.downstream_task_ids:
            dot.edge(task.task_id, downstream_task_id)
    return dot
