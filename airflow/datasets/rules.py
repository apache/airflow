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
from typing import Collection

import attr
from sqlalchemy import func, case
from sqlalchemy.orm import Query

from airflow.datasets import Dataset
from airflow.models.dataset import DagScheduleDatasetReference, DatasetDagRunQueue


@attr.define
class Rule:
    datasets: Collection[Dataset]

    def __init__(self, datasets: Collection[Dataset]):
        self.datasets = datasets

    def filter_query(self, q: Query, ref: DagScheduleDatasetReference, queue: DatasetDagRunQueue) -> Query:
        return q

    def apply(self, **kwargs) -> Collection[Dataset]:
        return self.datasets


class AllOf(Rule):
    def filter_query(self, q: Query, ref: DagScheduleDatasetReference, queue: DatasetDagRunQueue) -> Query:
        q.having(func.count() == func.sum(case((queue.target_dag_id.is_not(None), 1), else_=0)))
        q.filter(DatasetDagRunQueue.dataset_id.in_(self.datasets))
        return q


class AnyOf(Rule):
    def filter_query(self, q: Query, ref: DagScheduleDatasetReference, queue: DatasetDagRunQueue) -> Query:
        q.having(func.count()-1 >= func.sum(case((queue.target_dag_id.is_not(None), 1), else_=0)))
        q.filter(DatasetDagRunQueue.dataset_id.in_(self.datasets))
        return q


def any_of(*datasets: Dataset) -> Rule:
    return AnyOf([*datasets])


def all_of(*datasets: Dataset) -> Rule:
    return AllOf([*datasets])
