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

from __future__ import annotations

from pathlib import Path

import airflow.dag_processing.parse_info as parse_info


def test_parse_bundle_info_hash():
    same_1 = parse_info.ParseBundleInfo(name="bundle-name", path="/root/path")
    same_2 = parse_info.ParseBundleInfo(name="bundle-name", path=Path("/root/path"))
    different_name = parse_info.ParseBundleInfo(name="different-name", path="/root/path")
    different_path = parse_info.ParseBundleInfo(name="bundle-name", path="/different/path")
    different_version = parse_info.ParseBundleInfo(name="bundle-name", path="/root/path", version="1.0.0")
    assert hash(same_1) == hash(same_2)
    assert hash(same_1) == hash(different_path)
    assert hash(same_1) != hash(different_name)
    assert hash(same_1) != hash(different_version)


def test_parse_bundle_info_eq():
    same_1 = parse_info.ParseBundleInfo(name="bundle-name", path="/root/path")
    same_2 = parse_info.ParseBundleInfo(name="bundle-name", path=Path("/root/path"))
    different_name = parse_info.ParseBundleInfo(name="different-name", path="/root/path")
    different_path = parse_info.ParseBundleInfo(name="bundle-name", path="/different/path")
    different_version = parse_info.ParseBundleInfo(name="bundle-name", path="/root/path", version="1.0.0")
    assert same_1 == same_2
    assert same_1 == different_path
    assert same_1 != different_name
    assert same_1 != different_version


def test_parse_file_info_hash():
    bundle = parse_info.ParseBundleInfo(name="bundle-name", path="/root/path")
    other_bundle = parse_info.ParseBundleInfo(name="other-bundle-name", path="/root/path")

    same_1 = parse_info.ParseFileInfo(rel_path="a", bundle=bundle)
    same_2 = parse_info.ParseFileInfo(rel_path=Path("a"), bundle=bundle)
    different_rel_path = parse_info.ParseFileInfo(rel_path="b", bundle=bundle)
    different_bundle = parse_info.ParseFileInfo(rel_path="a", bundle=other_bundle)

    assert hash(same_1) == hash(same_2)
    assert hash(same_1) != hash(different_rel_path)
    assert hash(same_1) != hash(different_bundle)


def test_parse_file_info_eq():
    bundle = parse_info.ParseBundleInfo(name="bundle-name", path="/root/path")
    other_bundle = parse_info.ParseBundleInfo(name="other-bundle-name", path="/root/path")

    same_1 = parse_info.ParseFileInfo(rel_path="a", bundle=bundle)
    same_2 = parse_info.ParseFileInfo(rel_path=Path("a"), bundle=bundle)
    different_rel_path = parse_info.ParseFileInfo(rel_path="b", bundle=bundle)
    different_bundle = parse_info.ParseFileInfo(rel_path="a", bundle=other_bundle)

    assert same_1 == same_2
    assert same_2 == same_1
    assert same_1 != different_rel_path
    assert different_rel_path != same_1
    assert same_1 != different_bundle
    assert different_bundle != same_1


def test_dag_entrypoint_hash():
    same_1 = parse_info.DagFile(rel_path="a/b", bundle_name="bundle-name")
    same_2 = parse_info.DagFile(rel_path=Path("a/b"), bundle_name="bundle-name")

    different_rel_path = parse_info.DagFile(rel_path="c/d", bundle_name="bundle-name")
    different_bundle_name = parse_info.DagFile(rel_path="a/b", bundle_name="other-bundle")

    assert hash(same_1) == hash(same_2)
    assert hash(same_1) != hash(different_rel_path)
    assert hash(same_1) != hash(different_bundle_name)


def test_dag_entrypoint_eq():
    same_1 = parse_info.DagFile(rel_path="a/b", bundle_name="bundle-name")
    same_2 = parse_info.DagFile(rel_path=Path("a/b"), bundle_name="bundle-name")

    different_rel_path = parse_info.DagFile(rel_path="c/d", bundle_name="bundle-name")
    different_bundle_name = parse_info.DagFile(rel_path="a/b", bundle_name="other-bundle")

    assert same_1 == same_2
    assert same_1 != different_rel_path
    assert same_1 != different_bundle_name
