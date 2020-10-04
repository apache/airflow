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

from typing import NamedTuple, Optional, List

from cached_property import cached_property

from airflow import conf
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.upgrade.rules.renamed_classes import ALL
from airflow.utils.dag_processing import list_py_file_paths


class ImportChange(
    NamedTuple(
        "ImportChange",
        [("old_path", str), ("new_path", str), ("providers_package", Optional[None])],
    )
):
    def info(self, file_path=None):
        msg = "Using `{}` will be replaced by `{}`".format(self.old_path, self.new_path)
        if self.providers_package:
            msg += " and requires `{}` providers package".format(
                self.providers_package
            )
        if file_path:
            msg += ". Affected file: {}".format(file_path)
        return msg

    @cached_property
    def old_class(self):
        return self.old_path.split(".")[-1]

    @cached_property
    def new_class(self):
        return self.new_path.split(".")[-1]

    @classmethod
    def from_new_old_paths(cls, new_path, old_path):
        providers_package = new_path.split(".")[2] if "providers" in new_path else None
        return cls(
            old_path=old_path, new_path=new_path, providers_package=providers_package
        )


class ImportChangesRule(BaseRule):
    title = "Changes in import paths of hooks, operators, sensors and others"
    description = (
        "Many hooks, operators and other classes has been renamed and moved. Those changes were part of "
        "unifying names and imports paths as described in AIP-21.\nThe `contrib` folder has been replaced "
        "by `providers` directory and packages:\n"
        "https://github.com/apache/airflow#backport-packages"
    )

    ALL_CHANGES = [
        ImportChange.from_new_old_paths(*args) for args in ALL
    ]  # type: List[ImportChange]

    @staticmethod
    def _check_file(file_path):
        problems = []
        with open(file_path, "r") as file:
            content = file.read()
            for change in ImportChangesRule.ALL_CHANGES:
                if change.old_class in content:
                    problems.append(change.info(file_path))
        return problems

    def check(self):
        dag_folder = conf.get("core", "dags_folder")
        files = list_py_file_paths(directory=dag_folder, include_examples=False)
        problems = []
        for file in files:
            problems.extend(self._check_file(file))
        return problems
