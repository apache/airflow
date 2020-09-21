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
        providers_package = new_path.split(".")[1] if "providers" in new_path else None
        return cls(
            old_path=old_path, new_path=new_path, providers_package=providers_package
        )


ALL_CHANGES = [
    ImportChange.from_new_old_paths(*args) for args in ALL
]  # type: List[ImportChange]


class ImportChangesRule(BaseRule):
    title = "Changes in import paths of hooks, operators, sensors and others"
    description = (
        "Many hooks, operators and other classes has been renamed and moved. Those changes were part of "
        "unifying names and imports paths as described in AIP-21.\nThe `contrib` folder has been replaced "
        "by `providers` directory and packages:\n"
        "https://github.com/apache/airflow#backport-packages"
    )

    @staticmethod
    def _check_file(file_path):
        problems = []
        with open(file_path, "r") as file:
            content = file.read()
            for change in ALL_CHANGES:
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
