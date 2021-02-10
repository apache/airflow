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
import argparse
import os
import textwrap
import token
from os.path import dirname
from shutil import copyfile, copytree, rmtree
from typing import List

from bowler import LN, TOKEN, Capture, Filename, Query
from fissix.fixer_util import Comma, KeywordArg, Name
from fissix.pytree import Leaf
from rich import print

from dev.provider_packages.prepare_provider_packages import (
    get_source_airflow_folder,
    get_source_providers_folder,
    get_target_folder,
    get_target_providers_folder,
    get_target_providers_package_folder,
)


def copy_provider_sources(backports: bool) -> None:
    """
    Copies provider sources to directory where they will be refactored.
    """

    def rm_build_dir() -> None:
        """
        Removes build directory.
        """
        build_dir = os.path.join(dirname(__file__), "build")
        if os.path.isdir(build_dir):
            print(f"\nRemoving the {build_dir} directory: ", end='')
            rmtree(build_dir)
            print("[green]OK[/]")

    def ignore_google_auth_backend(src: str, names: List[str]) -> List[str]:
        del names
        if src.endswith("google" + os.path.sep + "common"):
            return ["auth_backend"]
        return []

    def ignore_some_files(src: str, names: List[str]) -> List[str]:
        ignored_list = []
        ignored_list.extend(ignore_google_auth_backend(src=src, names=names))
        return ignored_list

    rm_build_dir()
    package_providers_dir = get_target_providers_folder()
    if os.path.isdir(package_providers_dir):
        print(f"\nRemoving the previous source tree from {package_providers_dir}: ", end='')
        rmtree(package_providers_dir)
        print("[green]OK[/]")

    print(f"\nCopying the whole source tree to {package_providers_dir}: ", end='')
    copytree(
        get_source_providers_folder(),
        get_target_providers_folder(),
        ignore=ignore_some_files if backports else None,
    )
    print("[green]OK[/]\n")


def copy_helper_py_file(target_file_path: str) -> None:
    """
    Copies. airflow/utils/helper.py to a new location within provider package

    The helper has two methods (chain, cross_downstream) that are moved from the original helper to
    'airflow.models.baseoperator'. so in 1.10 they should reimport the original 'airflow.utils.helper'
    methods. Those deprecated methods use import with import_string("<IMPORT>") so it is easier to
    replace them as strings rather than with Bowler

    :param target_file_path: target path name for the helpers.py
    """

    source_helper_file_path = os.path.join(get_source_airflow_folder(), "airflow", "utils", "helpers.py")

    with open(source_helper_file_path) as in_file:
        with open(target_file_path, "wt") as out_file:
            for line in in_file:
                out_file.write(line.replace('airflow.models.baseoperator', 'airflow.utils.helpers'))


class RefactorBackportPackages:
    """
    Refactors the code of providers, so that it works in 1.10.

    """

    def __init__(self):
        self.qry = Query()

    def remove_class(self, class_name) -> None:
        """
        Removes class altogether. Example diff generated:

        .. code-block:: diff

            --- ./airflow/providers/qubole/example_dags/example_qubole.py
            +++ ./airflow/providers/qubole/example_dags/example_qubole.py
            @@ -22,7 +22,7 @@

             from airflow import DAG
             from airflow.operators.dummy_operator import DummyOperator
            -from airflow.operators.python import BranchPythonOperator, PythonOperator
            +from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
             from airflow.providers.qubole.operators.qubole import QuboleOperator
             from airflow.providers.qubole.sensors.qubole import QuboleFileSensor, QubolePartitionSensor
             from airflow.utils.dates import days_ago

        :param class_name: name to remove
        """

        def _remover(node: LN, capture: Capture, filename: Filename) -> None:
            node.remove()

        self.qry.select_class(class_name).modify(_remover)

    def rename_deprecated_modules(self) -> None:
        """
        Renames back to deprecated modules imported. Example diff generated:

        .. code-block:: diff

            --- ./airflow/providers/dingding/operators/dingding.py
            +++ ./airflow/providers/dingding/operators/dingding.py
            @@ -16,7 +16,7 @@
             # specific language governing permissions and limitations
             # under the License.

            -from airflow.operators.baseoperator import BaseOperator
            +from airflow.operators.bash_operator import BaseOperator
             from airflow.providers.dingding.hooks.dingding import DingdingHook
             from airflow.utils.decorators import apply_defaults

        """
        changes = [
            ("airflow.hooks.base", "airflow.hooks.base_hook"),
            ("airflow.hooks.dbapi", "airflow.hooks.dbapi_hook"),
            ("airflow.operators.bash", "airflow.operators.bash_operator"),
            ("airflow.operators.branch", "airflow.operators.branch_operator"),
            ("airflow.operators.dummy", "airflow.operators.dummy_operator"),
            ("airflow.operators.python", "airflow.operators.python_operator"),
            ("airflow.operators.trigger_dagrun", "airflow.operators.dagrun_operator"),
            ("airflow.sensors.base", "airflow.sensors.base_sensor_operator"),
            ("airflow.sensors.date_time", "airflow.sensors.date_time_sensor"),
            ("airflow.sensors.external_task", "airflow.sensors.external_task_sensor"),
            ("airflow.sensors.sql", "airflow.sensors.sql_sensor"),
            ("airflow.sensors.time_delta", "airflow.sensors.time_delta_sensor"),
            ("airflow.sensors.weekday", "airflow.contrib.sensors.weekday_sensor"),
            ("airflow.utils.session", "airflow.utils.db"),
        ]
        for new, old in changes:
            self.qry.select_module(new).rename(old)

        def is_not_k8spodop(node: LN, capture: Capture, filename: Filename) -> bool:
            return not filename.endswith("/kubernetes_pod.py")

        self.qry.select_module("airflow.providers.cncf.kubernetes.backcompat").filter(
            callback=is_not_k8spodop
        ).rename("airflow.kubernetes")

        self.qry.select_module("airflow.providers.cncf.kubernetes.backcompat.pod_runtime_info_env").rename(
            "airflow.kubernetes.pod_runtime_info_env"
        )

        backcompat_target_folder = os.path.join(
            get_target_providers_package_folder("cncf.kubernetes"), "backcompat"
        )
        # Remove backcompat classes that are imported from "airflow.kubernetes"
        for file in ['pod.py', 'pod_runtime_info_env.py', 'volume.py', 'volume_mount.py']:
            os.remove(os.path.join(backcompat_target_folder, file))

    def add_provide_context_to_python_operators(self) -> None:
        """

        Adds provide context to usages of Python/BranchPython Operators - mostly in example_dags.
        Note that those changes  apply to example DAGs not to the operators/hooks erc.
        We package the example DAGs together with the provider classes and they should serve as
        examples independently on the version of Airflow it will be installed in.
        Provide_context feature in Python operators was feature added 2.0.0 and we are still
        using the "Core" operators from the Airflow version that the provider packages are installed
        in - the "Core" operators do not have (for now) their own provider package.

        The core operators are:

            * Python
            * BranchPython
            * Bash
            * Branch
            * Dummy
            * LatestOnly
            * ShortCircuit
            * PythonVirtualEnv


        Example diff generated:

        .. code-block:: diff

            --- ./airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_advanced.py
            +++ ./airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_advanced.py
            @@ -105,7 +105,8 @@
                         task_video_ids_to_s3.google_api_response_via_xcom,
                         task_video_ids_to_s3.task_id
                     ],
            -        task_id='check_and_transform_video_ids'
            +        task_id='check_and_transform_video_ids',
            +        provide_context=True
                 )

        """

        def add_provide_context_to_python_operator(node: LN, capture: Capture, filename: Filename) -> None:
            fn_args = capture['function_arguments'][0]
            if len(fn_args.children) > 0 and (
                not isinstance(fn_args.children[-1], Leaf) or fn_args.children[-1].type != token.COMMA
            ):
                fn_args.append_child(Comma())

            provide_context_arg = KeywordArg(Name('provide_context'), Name('True'))
            provide_context_arg.prefix = fn_args.children[0].prefix
            fn_args.append_child(provide_context_arg)

        (self.qry.select_function("PythonOperator").is_call().modify(add_provide_context_to_python_operator))
        (
            self.qry.select_function("BranchPythonOperator")
            .is_call()
            .modify(add_provide_context_to_python_operator)
        )

    def remove_super_init_call(self):
        r"""
        Removes super().__init__() call from Hooks.

        In airflow 1.10 almost none of the Hooks call super().init(). It was always broken in Airflow 1.10 -
        the BaseHook() has it's own __init__() which is wrongly implemented and requires source
        parameter to be passed::

        .. code-block:: python

            def __init__(self, source):
                pass

        We fixed it in 2.0, but for the entire 1.10 line calling super().init() is not a good idea -
        and it basically does nothing even if you do. And it's bad because it does not initialize
        LoggingMixin (BaseHook derives from LoggingMixin). And it is the main reason why Hook
        logs are not working as they are supposed to sometimes:

        .. code-block:: python

            class LoggingMixin(object):
                \"\"\"
                Convenience super-class to have a logger configured with the class name
                \"\"\"
                def __init__(self, context=None):
                    self._set_context(context)


        There are two Hooks in 1.10 that call super.__init__ :

        .. code-block:: python

               super(CloudSqlDatabaseHook, self).__init__(source=None)
               super(MongoHook, self).__init__(source='mongo')

        Not that it helps with anything because init in BaseHook does nothing. So we remove
        the super().init() in Hooks when backporting to 1.10.

        Example diff generated:

        .. code-block:: diff

            --- ./airflow/providers/apache/druid/hooks/druid.py
            +++ ./airflow/providers/apache/druid/hooks/druid.py
            @@ -49,7 +49,7 @@
                         timeout=1,
                         max_ingestion_time=None):

            -        super().__init__()
            +
                     self.druid_ingest_conn_id = druid_ingest_conn_id
                     self.timeout = timeout
                     self.max_ingestion_time = max_ingestion_time

        """

        def remove_super_init_call_modifier(node: LN, capture: Capture, filename: Filename) -> None:
            for ch in node.post_order():
                if isinstance(ch, Leaf) and ch.value == "super":
                    if any(c.value for c in ch.parent.post_order() if isinstance(c, Leaf)):
                        ch.parent.remove()

        self.qry.select_subclass("BaseHook").modify(remove_super_init_call_modifier)

    def remove_tags(self):
        """
        Removes tags from execution of the operators (in example_dags). Note that those changes
        apply to example DAGs not to the operators/hooks erc. We package the example DAGs together
        with the provider classes and they should serve as examples independently on the version
        of Airflow it will be installed in. The tags are feature added in 1.10.10 and occasionally
        we will want to run example DAGs as system tests in pre-1.10.10 version so we want to
        remove the tags here.


        Example diff generated:

        .. code-block:: diff


            -- ./airflow/providers/amazon/aws/example_dags/example_datasync_2.py
            +++ ./airflow/providers/amazon/aws/example_dags/example_datasync_2.py
            @@ -83,8 +83,7 @@
             with models.DAG(
                 "example_datasync_2",
                 default_args=default_args,
            -    schedule_interval=None,  # Override to match your needs
            -    tags=['example'],
            +    schedule_interval=None,
             ) as dag:

        """

        def remove_tags_modifier(_: LN, capture: Capture, filename: Filename) -> None:
            for node in capture['function_arguments'][0].post_order():
                if isinstance(node, Leaf) and node.value == "tags" and node.type == TOKEN.NAME:
                    if node.parent.next_sibling and node.parent.next_sibling.value == ",":
                        node.parent.next_sibling.remove()
                    node.parent.remove()

        # Remove tags
        self.qry.select_method("DAG").is_call().modify(remove_tags_modifier)

    def remove_poke_mode_only_decorator(self):
        r"""
        Removes @poke_mode_only decorator. The decorator is only available in Airflow 2.0.

        Example diff generated:

        .. code-block:: diff

            --- ./airflow/providers/google/cloud/sensors/gcs.py
            +++ ./airflow/providers/google/cloud/sensors/gcs.py
            @@ -189,7 +189,6 @@
                 return datetime.now()


            -@poke_mode_only
             class GCSUploadSessionCompleteSensor(BaseSensorOperator):
                 \"\"\"
                Checks for changes in the number of objects at prefix in Google Cloud Storage

        """

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
            return (
                node.children
                and len(node.children) >= 2
                and isinstance(node.children[0], Leaf)
                and node.children[0].value == '@'
                and isinstance(node.children[1], Leaf)
                and node.children[1].value == 'poke_mode_only'
            )

        def remove_poke_mode_only_modifier(node: LN, capture: Capture, filename: Filename) -> None:
            for child in capture['node'].parent.children:
                if is_poke_mode_only_decorator(child):
                    find_root_remove_import(child)
                    child.remove()

        self.qry.select_subclass("BaseSensorOperator").modify(remove_poke_mode_only_modifier)

    def refactor_amazon_package(self):
        """
        Fixes to "amazon" providers package.

        Copies some of the classes used from core Airflow to "common.utils" package of
        the provider and renames imports to use them from there.

        We copy typing_compat.py and change import as in example diff:

        .. code-block:: diff

            --- ./airflow/providers/amazon/aws/operators/ecs.py
            +++ ./airflow/providers/amazon/aws/operators/ecs.py
            @@ -24,7 +24,7 @@
             from airflow.models import BaseOperator
             from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
             from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
            -from airflow.typing_compat import Protocol, runtime_checkable
            +from airflow.providers.amazon.common.utils.typing_compat import Protocol, runtime_checkable
             from airflow.utils.decorators import apply_defaults

        """

        def amazon_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/amazon/")

        os.makedirs(
            os.path.join(get_target_providers_package_folder("amazon"), "common", "utils"), exist_ok=True
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("amazon"), "common", "__init__.py"),
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("amazon"), "common", "utils", "__init__.py"),
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "typing_compat.py"),
            os.path.join(
                get_target_providers_package_folder("amazon"), "common", "utils", "typing_compat.py"
            ),
        )
        (
            self.qry.select_module("airflow.typing_compat")
            .filter(callback=amazon_package_filter)
            .rename("airflow.providers.amazon.common.utils.typing_compat")
        )

        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "email.py"),
            os.path.join(get_target_providers_package_folder("amazon"), "common", "utils", "email.py"),
        )
        (
            self.qry.select_module("airflow.utils.email")
            .filter(callback=amazon_package_filter)
            .rename("airflow.providers.amazon.common.utils.email")
        )

    def refactor_elasticsearch_package(self):
        """
        Fixes to "elasticsearch" providers package.

        Copies some of the classes used from core Airflow to "common.utils" package of
        the provider and renames imports to use them from there.

        We copy file_task_handler.py and change import as in example diff:

        .. code-block:: diff

            --- ./airflow/providers/elasticsearch/log/es_task_handler.py
            +++ ./airflow/providers/elasticsearch/log/es_task_handler.py
            @@ -24,7 +24,7 @@
            from airflow.configuration import conf
            from airflow.models import TaskInstance
            from airflow.utils import timezone
            from airflow.utils.helpers import parse_template_string
            -from airflow.utils.log.file_task_handler import FileTaskHandler
            +from airflow.providers.elasticsearch.common.utils.log.file_task_handler import FileTaskHandler
            from airflow.utils.log.json_formatter import JSONFormatter
            from airflow.utils.log.logging_mixin import LoggingMixin

        """

        def elasticsearch_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/elasticsearch/")

        os.makedirs(
            os.path.join(get_target_providers_package_folder("elasticsearch"), "common", "utils", "log"),
            exist_ok=True,
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("elasticsearch"), "common", "__init__.py"),
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(
                get_target_providers_package_folder("elasticsearch"), "common", "utils", "__init__.py"
            ),
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "log", "__init__.py"),
            os.path.join(
                get_target_providers_package_folder("elasticsearch"), "common", "utils", "log", "__init__.py"
            ),
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "log", "file_task_handler.py"),
            os.path.join(
                get_target_providers_package_folder("elasticsearch"),
                "common",
                "utils",
                "log",
                "file_task_handler.py",
            ),
        )
        (
            self.qry.select_module("airflow.utils.log.file_task_handler")
            .filter(callback=elasticsearch_package_filter)
            .rename("airflow.providers.elasticsearch.common.utils.log.file_task_handler")
        )

    def refactor_google_package(self):
        r"""
        Fixes to "google" providers package.

        Copies some of the classes used from core Airflow to "common.utils" package of the
        the provider and renames imports to use them from there. Note that in this case we also rename
        the imports in the copied files.

        For example we copy python_virtualenv.py, process_utils.py and change import as in example diff:

        .. code-block:: diff

            --- ./airflow/providers/google/cloud/operators/kubernetes_engine.py
            +++ ./airflow/providers/google/cloud/operators/kubernetes_engine.py
            @@ -28,11 +28,11 @@

             from airflow.exceptions import AirflowException
             from airflow.models import BaseOperator
            -from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
            +from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
             from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEHook
             from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
             from airflow.utils.decorators import apply_defaults
            -from airflow.utils.process_utils import execute_in_subprocess, patch_environ
            +from airflow.providers.google.common.utils.process_utils import execute_in_subprocess


        And in the copied python_virtualenv.py we also change import to process_utils.py. This happens
        automatically and is solved by Pybowler.


        .. code-block:: diff

            --- ./airflow/providers/google/common/utils/python_virtualenv.py
            +++ ./airflow/providers/google/common/utils/python_virtualenv.py
            @@ -21,7 +21,7 @@
             \"\"\"
            from typing import List, Optional

            -from airflow.utils.process_utils import execute_in_subprocess
            +from airflow.providers.google.common.utils.process_utils import execute_in_subprocess


            def _generate_virtualenv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool)


        We also rename Base operator links to deprecated names:


        .. code-block:: diff

            --- ./airflow/providers/google/cloud/operators/mlengine.py
            +++ ./airflow/providers/google/cloud/operators/mlengine.py
            @@ -24,7 +24,7 @@
             from typing import List, Optional

             from airflow.exceptions import AirflowException
            -from airflow.models import BaseOperator, BaseOperatorLink
            +from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
             from airflow.models.taskinstance import TaskInstance
             from airflow.providers.google.cloud.hooks.mlengine import MLEngineHook
             from airflow.utils.decorators import apply_defaults

        We also copy (google.common.utils) and rename imports to the helpers.

        .. code-block:: diff

            --- ./airflow/providers/google/cloud/example_dags/example_datacatalog.py
            +++ ./airflow/providers/google/cloud/example_dags/example_datacatalog.py
            @@ -37,7 +37,7 @@
                 CloudDataCatalogUpdateTagTemplateOperator,
             )
             from airflow.utils.dates import days_ago
            -from airflow.utils.helpers import chain
            +from airflow.providers.google.common.utils.helpers import chain

             default_args = {"start_date": days_ago(1)}

        And also module_loading  which is used by helpers

        .. code-block:: diff

            --- ./airflow/providers/google/common/utils/helpers.py
            +++ ./airflow/providers/google/common/utils/helpers.py
            @@ -26,7 +26,7 @@
             from jinja2 import Template

             from airflow.exceptions import AirflowException
            -from airflow.utils.module_loading import import_string
            +from airflow.providers.google.common.utils.module_loading import import_string

             KEY_REGEX = re.compile(r'^[\\w.-]+$')

        """

        def google_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/google/")

        def pure_airflow_models_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            """Check if select is exactly [airflow, . , models]"""
            return len(list(node.children[1].leaves())) == 3

        def _contains_chain_in_import_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            if "module_import" in capture:
                return bool("chain" in capture["module_import"].value) and filename.startswith(
                    "./airflow/providers/google/"
                )
            return False

        os.makedirs(
            os.path.join(get_target_providers_package_folder("google"), "common", "utils"), exist_ok=True
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("google"), "common", "utils", "__init__.py"),
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "python_virtualenv.py"),
            os.path.join(
                get_target_providers_package_folder("google"), "common", "utils", "python_virtualenv.py"
            ),
        )

        copy_helper_py_file(
            os.path.join(get_target_providers_package_folder("google"), "common", "utils", "helpers.py")
        )

        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "module_loading.py"),
            os.path.join(
                get_target_providers_package_folder("google"), "common", "utils", "module_loading.py"
            ),
        )
        (
            self.qry.select_module("airflow.utils.python_virtualenv")
            .filter(callback=google_package_filter)
            .rename("airflow.providers.google.common.utils.python_virtualenv")
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "process_utils.py"),
            os.path.join(
                get_target_providers_package_folder("google"), "common", "utils", "process_utils.py"
            ),
        )
        (
            self.qry.select_module("airflow.utils.process_utils")
            .filter(callback=google_package_filter)
            .rename("airflow.providers.google.common.utils.process_utils")
        )

        (
            self.qry.select_module("airflow.models.baseoperator")
            .filter(callback=_contains_chain_in_import_filter)
            .rename("airflow.providers.google.common.utils.helpers")
        )

        (
            self.qry.select_module("airflow.utils.helpers")
            .filter(callback=google_package_filter)
            .rename("airflow.providers.google.common.utils.helpers")
        )

        (
            self.qry.select_module("airflow.utils.module_loading")
            .filter(callback=google_package_filter)
            .rename("airflow.providers.google.common.utils.module_loading")
        )

        (
            # Fix BaseOperatorLinks imports
            self.qry.select_module("airflow.models")
            .is_filename(include=r"bigquery\.py|mlengine\.py")
            .filter(callback=google_package_filter)
            .filter(pure_airflow_models_filter)
            .rename("airflow.models.baseoperator")
        )

    def refactor_apache_beam_package(self):
        r"""
        Fixes to "apache_beam" providers package.

        Copies some of the classes used from core Airflow to "common.utils" package of the
        the provider and renames imports to use them from there. Note that in this case we also rename
        the imports in the copied files.

        For example we copy python_virtualenv.py, process_utils.py and change import as in example diff:

        .. code-block:: diff

            --- ./airflow/providers/apache/beam/common/utils/python_virtualenv.py
            +++ ./airflow/providers/apache/beam/common/utils/python_virtualenv.py
            @@ -21,7 +21,7 @@
             \"\"\"
            from typing import List, Optional

            -from airflow.utils.process_utils import execute_in_subprocess
            +from airflow.providers.apache.beam.common.utils.process_utils import execute_in_subprocess


            def _generate_virtualenv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool)

        """

        def apache_beam_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/apache/beam")

        os.makedirs(
            os.path.join(get_target_providers_package_folder("apache.beam"), "common", "utils"), exist_ok=True
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(
                get_target_providers_package_folder("apache.beam"), "common", "utils", "__init__.py"
            ),
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "python_virtualenv.py"),
            os.path.join(
                get_target_providers_package_folder("apache.beam"), "common", "utils", "python_virtualenv.py"
            ),
        )
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "process_utils.py"),
            os.path.join(
                get_target_providers_package_folder("apache.beam"), "common", "utils", "process_utils.py"
            ),
        )
        (
            self.qry.select_module("airflow.utils.python_virtualenv")
            .filter(callback=apache_beam_package_filter)
            .rename("airflow.providers.apache.beam.common.utils.python_virtualenv")
        )
        (
            self.qry.select_module("airflow.utils.process_utils")
            .filter(callback=apache_beam_package_filter)
            .rename("airflow.providers.apache.beam.common.utils.process_utils")
        )

    def refactor_odbc_package(self):
        """
        Fixes to "odbc" providers package.

        Copies some of the classes used from core Airflow to "common.utils" package of the
        the provider and renames imports to use them from there.

        We copy helpers.py and change import as in example diff:

        .. code-block:: diff

            --- ./airflow/providers/google/cloud/example_dags/example_datacatalog.py
            +++ ./airflow/providers/google/cloud/example_dags/example_datacatalog.py
            @@ -37,7 +37,7 @@
                 CloudDataCatalogUpdateTagTemplateOperator,
             )
             from airflow.utils.dates import days_ago
            -from airflow.utils.helpers import chain
            +from airflow.providers.odbc.utils.helpers import chain

             default_args = {"start_date": days_ago(1)}


        """

        def odbc_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/odbc/")

        os.makedirs(os.path.join(get_target_providers_folder(), "odbc", "utils"), exist_ok=True)
        copyfile(
            os.path.join(get_source_airflow_folder(), "airflow", "utils", "__init__.py"),
            os.path.join(get_target_providers_package_folder("odbc"), "utils", "__init__.py"),
        )
        copy_helper_py_file(os.path.join(get_target_providers_package_folder("odbc"), "utils", "helpers.py"))

        (
            self.qry.select_module("airflow.utils.helpers")
            .filter(callback=odbc_package_filter)
            .rename("airflow.providers.odbc.utils.helpers")
        )

    def refactor_kubernetes_pod_operator(self):
        def kubernetes_package_filter(node: LN, capture: Capture, filename: Filename) -> bool:
            return filename.startswith("./airflow/providers/cncf/kubernetes")

        (
            self.qry.select_class("KubernetesPodOperator")
            .select_method("add_xcom_sidecar")
            .filter(callback=kubernetes_package_filter)
            .rename("add_sidecar")
        )

    def do_refactor(self, in_process: bool = False) -> None:  # noqa
        print("Perform refactoring.", end='')
        self.rename_deprecated_modules()
        self.refactor_amazon_package()
        self.refactor_google_package()
        self.refactor_apache_beam_package()
        self.refactor_elasticsearch_package()
        self.refactor_odbc_package()
        self.remove_tags()
        self.remove_super_init_call()
        self.add_provide_context_to_python_operators()
        self.remove_poke_mode_only_decorator()
        self.refactor_kubernetes_pod_operator()
        # In order to debug Bowler - set in_process to True
        self.qry.execute(write=True, silent=False, interactive=False, in_process=in_process)
        print("[green]OK[/]")


def get_parser():
    cli_parser = argparse.ArgumentParser(
        description="Copies sources and optionally refactors provider code to be Airflow 1.10 compatible.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    cli_parser.add_argument(
        "--backports",
        action='store_true',
        help=textwrap.dedent("Includes refactoring to prepare backport packages rather than regular ones"),
    )
    cli_parser.add_argument(
        "--debug",
        action='store_true',
        help=textwrap.dedent(
            "Run bowler refactoring in single process. Makes it debuggable with regular"
            " IDE debugger (much slower)"
        ),
    )
    return cli_parser


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()

    copy_provider_sources(args.backports)
    os.chdir(get_target_folder())
    if args.backports:
        print("\nRefactoring code to be Airflow 1.10 - compatible\n")
        RefactorBackportPackages().do_refactor(in_process=args.debug)
        print("\n[green]Refactored code successfully[/]\n")
