#!/usr/bin/env python3
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

import os
from os.path import dirname
from shutil import copyfile, copytree, rmtree
from typing import List

from backport_packages.setup_backport_packages import (
    get_source_airflow_folder, get_source_providers_folder, get_target_providers_folder,
    get_target_providers_package_folder, is_bigquery_non_dts_module,
)
from bowler import LN, TOKEN, Capture, Filename, Query
from fissix.fixer_util import Comma, KeywordArg, Name
from fissix.pytree import Leaf

CLASS_TYPES = ["hooks", "operators", "sensors", "secrets", "protocols"]


def copy_provider_sources() -> None:
    """
    Copies provider sources to directory where they will be refactored.
    """
    def rm_build_dir() -> None:
        """
        Removes build directory.
        """
        build_dir = os.path.join(dirname(__file__), "build")
        if os.path.isdir(build_dir):
            rmtree(build_dir)

    def ignore_bigquery_files(src: str, names: List[str]) -> List[str]:
        """
        Ignore files with bigquery
        :param src: source file
        :param names: Name of the file
        :return:
        """
        ignored_names = []
        if any([src.endswith(os.path.sep + class_type) for class_type in CLASS_TYPES]):
            ignored_names = [name for name in names
                             if is_bigquery_non_dts_module(module_name=name)]
        if src.endswith(os.path.sep + "example_dags"):
            for file_name in names:
                file_path = src + os.path.sep + file_name
                with open(file_path, "rt") as file:
                    text = file.read()
                if any([f"airflow.providers.google.cloud.{class_type}.bigquery" in text
                        for class_type in CLASS_TYPES]) or "_to_bigquery" in text:
                    print(f"Ignoring {file_path}")
                    ignored_names.append(file_name)
        return ignored_names

    def ignore_kubernetes_files(src: str, names: List[str]) -> List[str]:
        ignored_names = []
        if src.endswith(os.path.sep + "example_dags"):
            for file_name in names:
                if "example_kubernetes" in file_name:
                    ignored_names.append(file_name)
        return ignored_names

    def ignore_some_files(src: str, names: List[str]) -> List[str]:
        ignored_list = ignore_bigquery_files(src=src, names=names)
        ignored_list.extend(ignore_kubernetes_files(src=src, names=names))
        return ignored_list

    rm_build_dir()
    package_providers_dir = get_target_providers_folder()
    if os.path.isdir(package_providers_dir):
        rmtree(package_providers_dir)
    copytree(get_source_providers_folder(), get_target_providers_folder(), ignore=ignore_some_files)


class RefactorBackportPackages:
    """
    Refactors the code of providers, so that it works in 1.10.

    """

    def __init__(self):
        self.qry = Query()

    def remove_class(self, class_name) -> None:
        # noinspection PyUnusedLocal
        def _remover(node: LN, capture: Capture, filename: Filename) -> None:
            if node.type not in (300, 311):  # remove only definition
                node.remove()

        self.qry.select_class(class_name).modify(_remover)

    def rename_deprecated_modules(self):
        changes = [
            ("airflow.operators.bash", "airflow.operators.bash_operator"),
            ("airflow.operators.python", "airflow.operators.python_operator"),
            ("airflow.utils.session", "airflow.utils.db"),
            (
                "airflow.providers.cncf.kubernetes.operators.kubernetes_pod",
                "airflow.contrib.operators.kubernetes_pod_operator"
            ),
        ]
        for new, old in changes:
            self.qry.select_module(new).rename(old)

    def add_provide_context_to_python_operators(self):
        # noinspection PyUnusedLocal
        def add_provide_context_to_python_operator(node: LN, capture: Capture, filename: Filename) -> None:
            fn_args = capture['function_arguments'][0]
            fn_args.append_child(Comma())

            provide_context_arg = KeywordArg(Name('provide_context'), Name('True'))
            provide_context_arg.prefix = fn_args.children[0].prefix
            fn_args.append_child(provide_context_arg)

        (
            self.qry.
            select_function("PythonOperator").
            is_call().
            is_filename(include=r"mlengine_operator_utils.py$").
            modify(add_provide_context_to_python_operator)
        )
        (
            self.qry.
            select_function("BranchPythonOperator").
            is_call().
            is_filename(include=r"example_google_api_to_s3_transfer_advanced.py$").
            modify(add_provide_context_to_python_operator)
        )

    def remove_super_init_call(self):
        # noinspection PyUnusedLocal
        def remove_super_init_call_modifier(node: LN, capture: Capture, filename: Filename) -> None:
            for ch in node.post_order():
                if isinstance(ch, Leaf) and ch.value == "super":
                    if any(c.value for c in ch.parent.post_order() if isinstance(c, Leaf)):
                        ch.parent.remove()

        self.qry.select_subclass("BaseHook").modify(remove_super_init_call_modifier)

    def remove_tags(self):
        # noinspection PyUnusedLocal
        def remove_tags_modifier(_: LN, capture: Capture, filename: Filename) -> None:
            for node in capture['function_arguments'][0].post_order():
                if isinstance(node, Leaf) and node.value == "tags" and node.type == TOKEN.NAME:
                    if node.parent.next_sibling and node.parent.next_sibling.value == ",":
                        node.parent.next_sibling.remove()
                    node.parent.remove()

        # Remove tags
        self.qry.select_method("DAG").is_call().modify(remove_tags_modifier)

    def remove_poke_mode_only_decorator(self):
        def find_and_remove_poke_mode_only_import(node: LN):
            for child in node.children:
                if isinstance(child, Leaf) and child.type == 1 and child.value == 'poke_mode_only':
                    import_node = child.parent
                    # remove the import by default
                    skip_import_remove = False
                    if isinstance(child.prev_sibling, Leaf) and child.prev_sibling.value == ",":
                        # remove coma before the whole import
                        child.prev_sibling.remove()
                        # do not remove if there are other imports
                        skip_import_remove = True
                    if isinstance(child.next_sibling, Leaf) and child.prev_sibling.value == ",":
                        # but keep the one after and do not remove the whole import
                        skip_import_remove = True
                    # remove the import
                    child.remove()
                    if not skip_import_remove:
                        # remove import of there were no sibling
                        import_node.remove()
                else:
                    find_and_remove_poke_mode_only_import(child)

        def find_root_remove_import(node: LN):
            current_node = node
            while current_node.parent:
                current_node = current_node.parent
            find_and_remove_poke_mode_only_import(current_node)

        def is_poke_mode_only_decorator(node: LN) -> bool:
            return node.children and len(node.children) >= 2 and \
                isinstance(node.children[0], Leaf) and node.children[0].value == '@' and \
                isinstance(node.children[1], Leaf) and node.children[1].value == 'poke_mode_only'

        # noinspection PyUnusedLocal
        def remove_poke_mode_only_modifier(node: LN, capture: Capture, filename: Filename) -> None:
            for child in capture['node'].parent.children:
                if is_poke_mode_only_decorator(child):
                    find_root_remove_import(child)
                    child.remove()

        self.qry.select_subclass("BaseSensorOperator").modify(remove_poke_mode_only_modifier)

    def refactor_amazon_package(self):
        # noinspection PyUnusedLocal
        def amazon_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/amazon/")

        os.makedirs(os.path.join(get_target_providers_package_folder("amazon"), "common", "utils"),
                    exist_ok=True)
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("amazon"), "common", "__init__.py")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("amazon"), "common", "utils", "__init__.py")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "typing_compat.py"),
            os.path.join(get_target_providers_package_folder("amazon"), "common", "utils", "typing_compat.py")
        )
        (
            self.qry.
            select_module("airflow.typing_compat").
            filter(callback=amazon_package_filter).
            rename("airflow.providers.amazon.common.utils.typing_compat")
        )

    def refactor_google_package(self):
        # noinspection PyUnusedLocal
        def google_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/google/")

        # noinspection PyUnusedLocal
        def pure_airflow_models_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            """Check if select is exactly [airflow, . , models]"""
            return len([ch for ch in node.children[1].leaves()]) == 3

        os.makedirs(os.path.join(get_target_providers_package_folder("google"), "common", "utils"),
                    exist_ok=True)
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("google"), "common", "utils", "__init__.py")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "python_virtualenv.py"),
            os.path.join(get_target_providers_package_folder("google"), "common", "utils",
                         "python_virtualenv.py")
        )
        (
            self.qry.
            select_module("airflow.utils.python_virtualenv").
            filter(callback=google_package_filter).
            rename("airflow.providers.google.common.utils.python_virtualenv")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "process_utils.py"),
            os.path.join(get_target_providers_package_folder("google"), "common", "utils", "process_utils.py")
        )
        (
            self.qry.
            select_module("airflow.utils.process_utils").
            filter(callback=google_package_filter).
            rename("airflow.providers.google.common.utils.process_utils")
        )

        (
            # Fix BaseOperatorLinks imports
            self.qry.select_module("airflow.models").
            is_filename(include=r"bigquery\.py|mlengine\.py").
            filter(callback=google_package_filter).
            filter(pure_airflow_models_filter).
            rename("airflow.models.baseoperator")
        )
        self.remove_class("GKEStartPodOperator")
        (
            self.qry.
            select_class("GKEStartPodOperator").
            is_filename(include=r"example_kubernetes_engine\.py").
            rename("GKEPodOperator")
        )

    def refactor_odbc_package(self):
        # noinspection PyUnusedLocal
        def odbc_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/odbc/")

        os.makedirs(os.path.join(get_target_providers_folder(), "odbc", "utils"), exist_ok=True)
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("odbc"), "utils", "__init__.py")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "helpers.py"),
            os.path.join(get_target_providers_package_folder("odbc"), "utils", "helpers.py")
        )
        (
            self.qry.
            select_module("airflow.utils.helpers").
            filter(callback=odbc_package_filter).
            rename("airflow.providers.odbc.utils.helpers")
        )

    def refactor_papermill_package(self):
        # noinspection PyUnusedLocal
        def papermill_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/papermill/")

        os.makedirs(os.path.join(get_target_providers_package_folder("papermill"), "utils", "lineage"),
                    exist_ok=True)
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("papermill"), "utils", "__init__.py")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "lineage", "__init__.py"),
            os.path.join(get_target_providers_package_folder("papermill"), "utils", "lineage", "__init__.py")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "lineage", "entities.py"),
            os.path.join(get_target_providers_package_folder("papermill"), "utils", "lineage", "entities.py")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "backport_packages", "template_base_operator.py.txt"),
            os.path.join(get_target_providers_package_folder("papermill"), "utils", "base.py")
        )
        (
            self.qry.
            select_module("airflow.lineage.entities").
            filter(callback=papermill_package_filter).
            rename("airflow.providers.papermill.utils.lineage.entities")
        )
        (
            self.qry.
            select_module("airflow.lineage").
            filter(callback=papermill_package_filter).
            rename("airflow.providers.papermill.utils.lineage")
        )
        # Papermill uses lineage which uses Operator under the hood so we need to change it as well
        (
            self.qry.
            select_module("airflow.models.base").
            filter(callback=papermill_package_filter).
            rename("airflow.providers.papermill.utils.base")
        )

    def do_refactor(self) -> None:
        self.rename_deprecated_modules()
        self.refactor_amazon_package()
        self.refactor_google_package()
        self.refactor_odbc_package()
        self.refactor_papermill_package()
        self.remove_tags()
        self.remove_super_init_call()
        self.add_provide_context_to_python_operators()
        self.remove_poke_mode_only_decorator()
        # In order to debug Bowler - set in_process to True
        self.qry.execute(write=True, silent=False, interactive=False, in_process=False)


if __name__ == '__main__':
    copy_provider_sources()
    RefactorBackportPackages().do_refactor()
