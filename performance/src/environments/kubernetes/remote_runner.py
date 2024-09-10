"""
Class dedicated to executing remote Airflow commands and python trampolines on Kubernetes pods.
"""

import ast
import inspect
import logging
import textwrap
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

from kubernetes import client
from kubernetes.stream import stream

from environments.kubernetes.airflow_pod_operations import (
    collect_dag_run_statistics,
    get_airflow_configuration,
    get_airflow_version,
    get_dags_count,
    get_dag_runs_count,
    get_python_version,
    unpause_dags,
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

DEFAULT_EXEC_REQUEST_TIMEOUT = 120.0
# TODO: in Airflow 2.0 logging level was moved to LOGGING section
PYTHON_TRAMPOLINE_TEMPLATE = textwrap.dedent(
    """
import logging
import os
os.environ["AIRFLOW__CORE__LOGGING_LEVEL"] = logging.getLevelName(logging.CRITICAL)
{func_code}
args = {args}
kwargs = {kwargs}
result = {func_name}(*args, **kwargs)
print(result)
"""
)


class RemoteRunner:
    """
    Class dedicated to executing payloads on found GKE pod
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        core_api: client.api.core_v1_api.CoreV1Api,
        namespace: str,
        pod_prefix: str,
        container: str,
        default_request_timeout: Optional[float] = None,
    ) -> None:
        """
        Creates an instance of RemoteRunner.

        :param core_api: an instance of kubernetes python core v1 api which can be used
            to execute payloads on pod.
        :type core_api: client.api.core_v1_api.CoreV1Api
        :param namespace: namespace the targeted pod is located on.
        :type namespace: str
        :param pod_prefix: prefix of the ready pod, on which payload should be executed.
        :type pod_prefix: str
        :param container: name of the container where payload should be executed.
        :type container: str
        :param default_request_timeout: the default time within which the remote commands executed
            by this RemoteRunner instance must finish. If not provided, it will be set to
            DEFAULT_EXEC_REQUEST_TIMEOUT seconds.
        :type default_request_timeout: float
        """

        self.core_api = core_api
        self.namespace = namespace
        self.pod_prefix = pod_prefix
        self.container = container
        self.default_request_timeout = default_request_timeout or DEFAULT_EXEC_REQUEST_TIMEOUT

    # pylint: enable=too-many-arguments

    def reset_airflow_database(self, request_timeout: Optional[float] = None) -> str:
        """
        Runs a remote command which resets Airflow database.

        :param request_timeout: time within which the connection must be closed
            and response returned. If not provided, the default value is used.
        :type request_timeout: float

        :return: string with the contents of the standard output of the pod
        :rtype: str
        """

        exec_command = ["airflow", "resetdb", "-y"]

        return self.run_command(exec_command, request_timeout)

    def get_dags_count(self, *args, **kwargs) -> int:
        """
        Runs get_dags_count payload on pod.
        This method should be executed with the same arguments as get_dags_count payload,
        provided either as positional or keyword arguments.

        :return: number of DAGs beginning with provided prefix found on instance.
        :rtype: int

        :raises: ValueError: if response from cluster could not be converted to integer type.
        """

        response = self.run_python_trampoline(get_dags_count, args, kwargs)

        try:
            return int(response)
        except ValueError as error:
            raise ValueError(f"Could not convert response from GKE with test DAGs count to int: {error}")

    def unpause_dags(self, *args, **kwargs) -> str:
        """
        Runs unpause_dags payload on pod.
        This method should be executed with the same arguments as unpause_dags payload,
        provided either as positional or keyword arguments.

        :return: string with the contents of the standard output of the pod
        :rtype: str
        """

        response = self.run_python_trampoline(unpause_dags, args, kwargs)

        return response

    def get_dag_runs_count(self, *args, **kwargs) -> int:
        """
        Runs get_dag_runs_count payload on pod and returns processed result.
        This method should be executed with the same arguments as get_dag_runs_count payload,
        provided either as positional or keyword arguments.

        :return: number of Dag Runs that have finished thus far.
        :rtype: int

        :raises: ValueError: if response from cluster could not be converted to int.
        """

        response = self.run_python_trampoline(get_dag_runs_count, args, kwargs)

        try:
            return int(response)
        except ValueError as error:
            raise ValueError(f"Could not convert response from GKE with Dag Run count to int: {error}")

    def collect_dag_run_statistics(self, *args, **kwargs) -> OrderedDict:
        """
        Runs collect_dag_run_statistics payload on pod and returns processed result.
        This method should be executed with the same arguments
        as collect_dag_run_statistics payload, provided either as positional or keyword arguments.

        :return: an OrderedDict with statistics of test Dag Runs execution in order
            in which they should appear in the results dataframe.
        :rtype: OrderedDict
        """

        response = self.run_python_trampoline(collect_dag_run_statistics, args, kwargs)

        # List is returned from k8s with single quotes, that is why there is no loading json here
        response_evaluated = OrderedDict(ast.literal_eval(response))
        return response_evaluated

    def get_airflow_configuration(self) -> OrderedDict:
        """
        Runs get_airflow_configuration payload on pod and returns processed result.

        :return: an OrderedDict with airflow configuration environment variables in order
            in which they should appear in the results dataframe.
        :rtype: OrderedDict
        """

        response = self.run_python_trampoline(get_airflow_configuration, (), {})

        # List is returned from k8s with single quotes, that is why there is no loading json here
        response_evaluated = OrderedDict(ast.literal_eval(response))
        return response_evaluated

    def get_python_version(self) -> str:
        """
        Runs a remote command which returns full version of python used on pod.

        :return: string with full version of python used on pod.
        :rtype: str
        """

        # this form of getting python version is used as it returns the version as simple string, so
        # no additional formatting of response is needed
        return self.run_python_trampoline(get_python_version, (), {})

    def get_airflow_version(self) -> str:
        """
        Runs get_airflow_version payload on pod and returns processed result.

        :return: a string with version of Apache Airflow installed on pod.
        :rtype: str
        """

        return self.run_python_trampoline(get_airflow_version, (), {})

    def run_python_trampoline(
        self,
        method: Callable,
        args: Tuple[Any, ...],
        kwargs: Dict,
        request_timeout: Optional[float] = None,
    ) -> str:
        """
        Prepares and runs the command which executes python method with specified arguments on pod.

        :param method: function to be executed on pod.
        :type method: Callable
        :param args: tuple with positional arguments of the method.
        :type args: Tuple[Any, ...]
        :param kwargs: dict with keyword arguments of the method.
        :type kwargs: Dict
        :param request_timeout: time within which the connection must be closed
            and response returned. If not provided, the default value is used.
        :type request_timeout: float

        :return: string with the contents of the standard output of the pod
        :rtype: str
        """

        python_code = self.fill_python_trampoline_template(method, args, kwargs)

        exec_command = ["python", "-c", python_code]

        return self.run_command(exec_command, request_timeout)

    @staticmethod
    def fill_python_trampoline_template(method: Callable, args: Tuple[Any, ...], kwargs: Dict) -> str:
        """
        Fills PYTHON_TRAMPOLINE_TEMPLATE with code of a method that is to be executed on pod
        as well as args and kwargs for it.

        :param method: function to be executed on pod.
        :type method: Callable
        :param args: tuple with positional arguments of the method.
        :type args: Tuple[Any, ...]
        :param kwargs: dict with keyword arguments of the method.
        :type kwargs: Dict

        :return: string with python code which, when run on pod, will execute the method with given
            args and kwargs and print the results to standard output
        :rtype: str
        """

        log.info("Filling the trampoline template for method: %s", method.__name__)

        python_code = PYTHON_TRAMPOLINE_TEMPLATE.format(
            func_code=inspect.getsource(method),
            func_name=method.__name__,
            args=args,
            kwargs=kwargs,
        )

        return python_code

    def run_command(self, exec_command: List[str], request_timeout: Optional[float] = None) -> str:
        """
        Executes given command on found pod and returns the results.

        :param exec_command: remote command to execute in form of a list of arguments.
        :type exec_command: List[str]
        :param request_timeout: time within which the connection must be closed
            and response returned. If not provided, the default value is used.
        :type request_timeout: float

        :return: string with the contents of the standard output of the pod
        :rtype: str

        :raises: ConnectionError
            if return code signifies an error
            if request_timeout has been reached and no response was returned
        """
        request_timeout = request_timeout or self.default_request_timeout

        # we search for pod every time as pod can be teared down and recreated
        pod = self.get_pod()

        log.info(
            "Executing remote command on pod '%s' from namespace '%s'.",
            pod,
            self.namespace,
        )

        response = stream(
            self.core_api.connect_get_namespaced_pod_exec,
            pod,
            self.namespace,
            container=self.container,
            command=exec_command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False,
        )

        response.run_forever(timeout=request_timeout)

        output = response.read_stdout().strip() if response.peek_stdout() else ""
        error = response.read_stderr().strip() if response.peek_stderr() else ""

        return_code = response.returncode

        response.close()

        log.info("stdout contents of remote command execution:\n%s", output)

        if return_code == 1:
            raise ConnectionError(
                f"Error encountered during remote command execution. " f"stderr contents:\n{error}"
            )
        if return_code is None:
            raise ConnectionError(
                f"It seems remote command did not finish within specified time "
                f"of {request_timeout} seconds. stderr contents:\n{error}"
            )
        if error:
            log.error("stderr contents of remote command execution:\n%s", error)

        return output

    def get_pod(self) -> str:
        """
        Finds the first ready pod with the given prefix in given namespace.

        :return: first of found pods.
        :rtype: str

        :raises: ValueError: if no ready pods were found.
        """

        log.info(
            "Collecting pod prefixed with '%s' from namespace '%s'.",
            self.pod_prefix,
            self.namespace,
        )

        response = self.core_api.list_namespaced_pod(self.namespace)

        metadata_list = [item["metadata"] for item in response.to_dict()["items"]]

        pods = [
            metadata["name"]
            for metadata in metadata_list
            if metadata["name"] and metadata["name"].startswith(self.pod_prefix)
        ]

        for pod in pods:
            resp = self.core_api.read_namespaced_pod(name=pod, namespace=self.namespace)
            if resp.status.phase == "Running":
                return pod

        raise ValueError(
            f"Could not find a ready pod starting with '{self.pod_prefix}' "
            f"in namespace '{self.namespace}'."
        )
