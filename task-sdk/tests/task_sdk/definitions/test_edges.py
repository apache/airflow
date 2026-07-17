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

from __future__ import annotations

import pytest

from airflow.sdk import DAG, Label
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.edges import EdgeInfoType, EdgeModifier


class TestLabelFactory:
    def test_returns_edge_modifier_with_label(self):
        modifier = Label("my label")
        assert isinstance(modifier, EdgeModifier)
        assert modifier.label == "my label"

    def test_edge_modifier_accepts_label_keyword(self):
        assert EdgeModifier(label="hi").label == "hi"


class TestEdgeModifierBasics:
    def test_default_label_is_none(self):
        assert EdgeModifier().label is None

    def test_new_modifier_has_empty_streams(self):
        modifier = EdgeModifier()
        assert modifier.roots == []
        assert modifier.leaves == []

    def test_roots_and_leaves_expose_streams(self):
        modifier = EdgeModifier()
        assert modifier.roots is modifier._downstream
        assert modifier.leaves is modifier._upstream


class TestMakeList:
    def test_wraps_single_non_sequence_item(self):
        item = object()
        assert EdgeModifier._make_list(item) == [item]

    @pytest.mark.parametrize("seq", [[object(), object()], (object(), object())], ids=["list", "tuple"])
    def test_passes_sequence_through_unchanged(self, seq):
        assert EdgeModifier._make_list(seq) is seq


class TestSaveNodes:
    def test_appends_valid_node(self):
        with DAG("test_save_nodes"):
            op = BaseOperator(task_id="op")
        modifier = EdgeModifier()
        modifier._save_nodes(op, modifier._downstream)
        assert modifier.roots == [op]

    def test_rejects_invalid_node_type(self):
        modifier = EdgeModifier()
        with pytest.raises(
            TypeError, match="Cannot use edge labels with int, only tasks, XComArg or TaskGroups"
        ):
            modifier._save_nodes(42, modifier._downstream)


class TestUpdateRelative:
    def test_upstream_populates_leaves(self):
        with DAG("test_update_relative_upstream"):
            op = BaseOperator(task_id="op")
        modifier = EdgeModifier()
        modifier.update_relative(op, upstream=True)
        assert modifier.leaves == [op]
        assert modifier.roots == []

    def test_downstream_populates_roots(self):
        with DAG("test_update_relative_downstream"):
            op = BaseOperator(task_id="op")
        modifier = EdgeModifier()
        modifier.update_relative(op, upstream=False)
        assert modifier.roots == [op]
        assert modifier.leaves == []


class TestEdgeLabelWiring:
    def test_label_between_two_operators_records_edge_info(self):
        with DAG("test_forward") as dag:
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")
            op1 >> Label("forward") >> op2

        assert op2 in op1.downstream_list
        assert dag.get_edge_info(upstream_task_id="op1", downstream_task_id="op2") == {"label": "forward"}

    def test_left_shift_records_edge_info_in_the_same_direction(self):
        with DAG("test_reverse") as dag:
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")
            op2 << Label("reverse") << op1

        assert op2 in op1.downstream_list
        assert dag.get_edge_info(upstream_task_id="op1", downstream_task_id="op2") == {"label": "reverse"}

    def test_label_fans_out_to_multiple_downstreams(self):
        with DAG("test_fanout") as dag:
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")
            op3 = BaseOperator(task_id="op3")
            op1 >> Label("fan") >> [op2, op3]

        assert {op2, op3} == set(op1.downstream_list)
        assert dag.get_edge_info(upstream_task_id="op1", downstream_task_id="op2") == {"label": "fan"}
        assert dag.get_edge_info(upstream_task_id="op1", downstream_task_id="op3") == {"label": "fan"}

    def test_add_edge_info_writes_label_to_dag(self):
        with DAG("test_add_edge_info") as dag:
            BaseOperator(task_id="up")
            BaseOperator(task_id="down")
        Label("direct").add_edge_info(dag, "up", "down")
        assert dag.get_edge_info(upstream_task_id="up", downstream_task_id="down") == {"label": "direct"}


def test_edge_info_type_only_defines_label():
    info: EdgeInfoType = {"label": "x"}
    assert info["label"] == "x"
    assert set(EdgeInfoType.__annotations__) == {"label"}
