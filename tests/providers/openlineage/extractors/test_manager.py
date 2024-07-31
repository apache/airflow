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

import pytest
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import documentation_dataset, ownership_dataset, schema_dataset

from airflow.lineage.entities import Column, File, Table, User
from airflow.providers.openlineage.extractors.manager import ExtractorManager


@pytest.mark.parametrize(
    ("uri", "dataset"),
    (
        ("s3://bucket1/dir1/file1", Dataset(namespace="s3://bucket1", name="dir1/file1")),
        ("gs://bucket2/dir2/file2", Dataset(namespace="gs://bucket2", name="dir2/file2")),
        ("gcs://bucket3/dir3/file3", Dataset(namespace="gs://bucket3", name="dir3/file3")),
        ("hdfs://namenodehost:8020/file1", Dataset(namespace="hdfs://namenodehost:8020", name="file1")),
        ("hdfs://namenodehost/file2", Dataset(namespace="hdfs://namenodehost", name="file2")),
        ("file://localhost/etc/fstab", Dataset(namespace="file://localhost", name="etc/fstab")),
        ("file:///etc/fstab", Dataset(namespace="file://", name="etc/fstab")),
        ("https://test.com", Dataset(namespace="https", name="test.com")),
        ("https://test.com?param1=test1&param2=test2", Dataset(namespace="https", name="test.com")),
        ("file:test.csv", None),
        ("not_an_url", None),
    ),
)
def test_convert_to_ol_dataset_from_object_storage_uri(uri, dataset):
    result = ExtractorManager.convert_to_ol_dataset_from_object_storage_uri(uri)
    assert result == dataset


@pytest.mark.parametrize(
    ("obj", "dataset"),
    (
        (
            Dataset(namespace="n1", name="f1"),
            Dataset(namespace="n1", name="f1"),
        ),
        (File(url="s3://bucket1/dir1/file1"), Dataset(namespace="s3://bucket1", name="dir1/file1")),
        (File(url="gs://bucket2/dir2/file2"), Dataset(namespace="gs://bucket2", name="dir2/file2")),
        (File(url="gcs://bucket3/dir3/file3"), Dataset(namespace="gs://bucket3", name="dir3/file3")),
        (
            File(url="hdfs://namenodehost:8020/file1"),
            Dataset(namespace="hdfs://namenodehost:8020", name="file1"),
        ),
        (File(url="hdfs://namenodehost/file2"), Dataset(namespace="hdfs://namenodehost", name="file2")),
        (File(url="file://localhost/etc/fstab"), Dataset(namespace="file://localhost", name="etc/fstab")),
        (File(url="file:///etc/fstab"), Dataset(namespace="file://", name="etc/fstab")),
        (File(url="https://test.com"), Dataset(namespace="https", name="test.com")),
        (Table(cluster="c1", database="d1", name="t1"), Dataset(namespace="c1", name="d1.t1")),
        ("gs://bucket2/dir2/file2", None),
        ("not_an_url", None),
    ),
)
def test_convert_to_ol_dataset(obj, dataset):
    result = ExtractorManager.convert_to_ol_dataset(obj)
    assert result == dataset


def test_convert_to_ol_dataset_from_table_with_columns_and_owners():
    table = Table(
        cluster="c1",
        database="d1",
        name="t1",
        columns=[
            Column(name="col1", description="desc1", data_type="type1"),
            Column(name="col2", description="desc2", data_type="type2"),
        ],
        owners=[
            User(email="mike@company.com", first_name="Mike", last_name="Smith"),
            User(email="theo@company.com", first_name="Theo"),
            User(email="smith@company.com", last_name="Smith"),
            User(email="jane@company.com"),
        ],
        description="test description",
    )
    expected_facets = {
        "schema": schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name="col1",
                    type="type1",
                    description="desc1",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="col2",
                    type="type2",
                    description="desc2",
                ),
            ]
        ),
        "ownership": ownership_dataset.OwnershipDatasetFacet(
            owners=[
                ownership_dataset.Owner(name="user:Mike Smith <mike@company.com>", type=""),
                ownership_dataset.Owner(name="user:Theo <theo@company.com>", type=""),
                ownership_dataset.Owner(name="user:Smith <smith@company.com>", type=""),
                ownership_dataset.Owner(name="user:<jane@company.com>", type=""),
            ]
        ),
        "documentation": documentation_dataset.DocumentationDatasetFacet(description="test description"),
    }
    result = ExtractorManager.convert_to_ol_dataset_from_table(table)
    assert result.namespace == "c1"
    assert result.name == "d1.t1"
    assert result.facets == expected_facets


def test_convert_to_ol_dataset_table():
    table = Table(
        cluster="c1",
        database="d1",
        name="t1",
        columns=[
            Column(name="col1", description="desc1", data_type="type1"),
            Column(name="col2", description="desc2", data_type="type2"),
        ],
        owners=[
            User(email="mike@company.com", first_name="Mike", last_name="Smith"),
            User(email="theo@company.com", first_name="Theo"),
            User(email="smith@company.com", last_name="Smith"),
            User(email="jane@company.com"),
        ],
    )
    expected_facets = {
        "schema": schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name="col1",
                    type="type1",
                    description="desc1",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="col2",
                    type="type2",
                    description="desc2",
                ),
            ]
        ),
        "ownership": ownership_dataset.OwnershipDatasetFacet(
            owners=[
                ownership_dataset.Owner(name="user:Mike Smith <mike@company.com>", type=""),
                ownership_dataset.Owner(name="user:Theo <theo@company.com>", type=""),
                ownership_dataset.Owner(name="user:Smith <smith@company.com>", type=""),
                ownership_dataset.Owner(name="user:<jane@company.com>", type=""),
            ]
        ),
    }

    result = ExtractorManager.convert_to_ol_dataset(table)
    assert result.namespace == "c1"
    assert result.name == "d1.t1"
    assert result.facets == expected_facets
