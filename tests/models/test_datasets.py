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

from airflow.models.dataset import Dataset, DatasetAll, DatasetAny, extract_datasets


def datasets_equal(d1, d2):
    if type(d1) != type(d2):
        return False

    if isinstance(d1, Dataset):
        return d1.uri == d2.uri

    elif isinstance(d1, (DatasetAny, DatasetAll)):
        if len(d1.objects) != len(d2.objects):
            return False

        # Compare each pair of objects
        for obj1, obj2 in zip(d1.objects, d2.objects):
            # If obj1 or obj2 is a Dataset, DatasetAny, or DatasetAll instance,
            # recursively call datasets_equal
            if not datasets_equal(obj1, obj2):
                return False
        return True

    return False


dataset1 = Dataset(uri="s3://bucket1/data1")
dataset2 = Dataset(uri="s3://bucket2/data2")
dataset3 = Dataset(uri="s3://bucket3/data3")
dataset4 = Dataset(uri="s3://bucket4/data4")
dataset5 = Dataset(uri="s3://bucket5/data5")

test_cases = [
    (lambda: dataset1, dataset1),
    (lambda: dataset1 & dataset2, DatasetAll(dataset1, dataset2)),
    (lambda: dataset1 | dataset2, DatasetAny(dataset1, dataset2)),
    (lambda: dataset1 | (dataset2 & dataset3), DatasetAny(dataset1, DatasetAll(dataset2, dataset3))),
    (lambda: dataset1 | dataset2 & dataset3, DatasetAny(dataset1, DatasetAll(dataset2, dataset3))),
    (
        lambda: ((dataset1 & dataset2) | dataset3) & (dataset4 | dataset5),
        DatasetAll(DatasetAny(DatasetAll(dataset1, dataset2), dataset3), DatasetAny(dataset4, dataset5)),
    ),
    (lambda: dataset1 & dataset2 | dataset3, DatasetAny(DatasetAll(dataset1, dataset2), dataset3)),
    (
        lambda: (dataset1 | dataset2) & (dataset3 | dataset4),
        DatasetAll(DatasetAny(dataset1, dataset2), DatasetAny(dataset3, dataset4)),
    ),
    (
        lambda: (dataset1 & dataset2) | (dataset3 & (dataset4 | dataset5)),
        DatasetAny(DatasetAll(dataset1, dataset2), DatasetAll(dataset3, DatasetAny(dataset4, dataset5))),
    ),
    (
        lambda: (dataset1 & dataset2) & (dataset3 & dataset4),
        DatasetAll(dataset1, dataset2, DatasetAll(dataset3, dataset4)),
    ),
    (lambda: dataset1 | dataset2 | dataset3, DatasetAny(dataset1, dataset2, dataset3)),
    (
        lambda: ((dataset1 & dataset2) | dataset3) & (dataset4 | dataset5),
        DatasetAll(DatasetAny(DatasetAll(dataset1, dataset2), dataset3), DatasetAny(dataset4, dataset5)),
    ),
]


@pytest.mark.parametrize("expression, expected", test_cases)
def test_extract_datasets(expression, expected):
    expr = expression()
    result = extract_datasets(expr)
    assert datasets_equal(result, expected)
