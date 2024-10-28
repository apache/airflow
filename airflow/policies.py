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

from typing import TYPE_CHECKING

import pluggy

local_settings_hookspec = pluggy.HookspecMarker("airflow.policy")
hookimpl = pluggy.HookimplMarker("airflow.policy")

__all__: list[str] = ["hookimpl"]

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG
    from airflow.models.taskinstance import TaskInstance


@local_settings_hookspec
def task_policy(task: BaseOperator) -> None:
    """
    Allow altering tasks after they are loaded in the DagBag.

    It allows administrator to rewire some task's parameters.  Alternatively you can raise
    ``AirflowClusterPolicyViolation`` exception to stop DAG from being executed.

    Here are a few examples of how this can be useful:

    * You could enforce a specific queue (say the ``spark`` queue) for tasks using the ``SparkOperator`` to
      make sure that these tasks get wired to the right workers
    * You could enforce a task timeout policy, making sure that no tasks run for more than 48 hours

    :param task: task to be mutated
    """


@local_settings_hookspec
def dag_policy(dag: DAG) -> None:
    """
    Allow altering DAGs after they are loaded in the DagBag.

    It allows administrator to rewire some DAG's parameters.
    Alternatively you can raise ``AirflowClusterPolicyViolation`` exception
    to stop DAG from being executed.

    Here are a few examples of how this can be useful:

    * You could enforce default user for DAGs
    * Check if every DAG has configured tags

    :param dag: dag to be mutated
    """


@local_settings_hookspec
def task_instance_mutation_hook(task_instance: TaskInstance) -> None:
    """
    Allow altering task instances before being queued by the Airflow scheduler.

    This could be used, for instance, to modify the task instance during retries.

    :param task_instance: task instance to be mutated
    """


@local_settings_hookspec
def pod_mutation_hook(pod) -> None:
    """
    Mutate pod before scheduling.

    This setting allows altering ``kubernetes.client.models.V1Pod`` object before they are passed to the
    Kubernetes client for scheduling.

    This could be used, for instance, to add sidecar or init containers to every worker pod launched by
    KubernetesExecutor or KubernetesPodOperator.
    """


@local_settings_hookspec(firstresult=True)
def get_airflow_context_vars(context) -> dict[str, str]:  # type: ignore[empty-body]
    """
    Inject airflow context vars into default airflow context vars.

    This setting allows getting the airflow context vars, which are key value pairs.  They are then injected
    to default airflow context vars, which in the end are available as environment variables when running
    tasks dag_id, task_id, execution_date, dag_run_id, try_number are reserved keys.

    :param context: The context for the task_instance of interest.
    """


@local_settings_hookspec(firstresult=True)
def get_dagbag_import_timeout(dag_file_path: str) -> int | float:  # type: ignore[empty-body]
    """
    Allow for dynamic control of the DAG file parsing timeout based on the DAG file path.

    It is useful when there are a few DAG files requiring longer parsing times, while others do not.
    You can control them separately instead of having one value for all DAG files.

    If the return value is less than or equal to 0, it means no timeout during the DAG parsing.
    """


class DefaultPolicy:
    """
    Default implementations of the policy functions.

    :meta private:
    """

    # Default implementations of the policy functions

    @staticmethod
    @hookimpl
    def get_dagbag_import_timeout(dag_file_path: str):
        from airflow.configuration import conf

        return conf.getfloat("core", "DAGBAG_IMPORT_TIMEOUT")

    @staticmethod
    @hookimpl
    def get_airflow_context_vars(context):
        return {}


def make_plugin_from_local_settings(pm: pluggy.PluginManager, module, names: set[str]):
    """
    Turn the functions from airflow_local_settings module into a custom/local plugin.

    Allows plugin-registered functions to co-operate with pluggy/setuptool
    entrypoint plugins of the same methods.

    Airflow local settings will be "win" (i.e. they have the final say) as they are the last plugin
    registered.

    :meta private:
    """
    import inspect
    import textwrap

    import attr

    hook_methods = set()

    def _make_shim_fn(name, desired_sig, target):
        # Functions defined in airflow_local_settings are called by positional parameters, so the names don't
        # have to match what we define in the "template" policy.
        #
        # However Pluggy validates the names match (and will raise an error if they don't!)
        #
        # To maintain compat, if we detect the names don't match, we will wrap it with a dynamically created
        # shim function that looks somewhat like this:
        #
        #  def dag_policy_name_mismatch_shim(dag):
        #      airflow_local_settings.dag_policy(dag)
        #
        codestr = textwrap.dedent(
            f"""
            def {name}_name_mismatch_shim{desired_sig}:
                return __target({' ,'.join(desired_sig.parameters)})
            """
        )
        code = compile(codestr, "<policy-shim>", "single")
        scope = {"__target": target}
        exec(code, scope, scope)
        return scope[f"{name}_name_mismatch_shim"]

    @attr.define(frozen=True)
    class AirflowLocalSettingsPolicy:
        hook_methods: tuple[str, ...]

        __name__ = "AirflowLocalSettingsPolicy"

        def __dir__(self):
            return self.hook_methods

    for name in names:
        if not hasattr(pm.hook, name):
            continue

        policy = getattr(module, name)

        if not policy:
            continue

        local_sig = inspect.signature(policy)
        policy_sig = inspect.signature(globals()[name])
        # We only care if names/order/number of parameters match, not type hints
        if local_sig.parameters.keys() != policy_sig.parameters.keys():
            policy = _make_shim_fn(name, policy_sig, target=policy)

        setattr(AirflowLocalSettingsPolicy, name, staticmethod(hookimpl(policy, specname=name)))
        hook_methods.add(name)

    if hook_methods:
        pm.register(AirflowLocalSettingsPolicy(hook_methods=tuple(hook_methods)))

    return hook_methods
