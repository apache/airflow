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

import base64
import inspect
import os
import pickle
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Callable, Dict, Iterable, List, Optional, TypeVar, Union

import dill

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.python_virtualenv import remove_task_decorator, write_python_script


def _generate_decode_command(env_var, file):
    # We don't need `f.close()` as the interpreter is about to exit anyway
    return (
        f'python -c "import base64, os;'
        rf'x = base64.b64decode(os.environ[\"{env_var}\"]);'
        rf'f = open(\"{file}\", \"wb\"); f.write(x);"'
    )


def _b64_encode_file(filename):
    with open(filename, "rb") as file_to_encode:
        return base64.b64encode(file_to_encode.read())


class _DockerDecoratedOperator(DecoratedOperator, DockerOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :type op_args: list
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :type multiple_outputs: bool
    """

    template_fields = ('op_args', 'op_kwargs')

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs = ('python_callable',)

    def __init__(
        self,
        use_dill=False,
        **kwargs,
    ) -> None:
        command = "dummy command"
        self.pickling_library = dill if use_dill else pickle
        super().__init__(
            command=command, retrieve_output=True, retrieve_output_path="/tmp/script.out", **kwargs
        )

    def execute(self, context: Dict):
        with TemporaryDirectory(prefix='venv') as tmp_dir:
            input_filename = os.path.join(tmp_dir, 'script.in')
            script_filename = os.path.join(tmp_dir, 'script.py')

            with open(input_filename, 'wb') as file:
                if self.op_args or self.op_kwargs:
                    self.pickling_library.dump({'args': self.op_args, 'kwargs': self.op_kwargs}, file)
            py_source = self._get_python_source()
            write_python_script(
                jinja_context=dict(
                    op_args=self.op_args,
                    op_kwargs=self.op_kwargs,
                    pickling_library=self.pickling_library.__name__,
                    python_callable=self.python_callable.__name__,
                    python_callable_source=py_source,
                    string_args_global=False,
                ),
                filename=script_filename,
            )

            # Pass the python script to be executed, and the input args, via environment variables. This is
            # more than slightly hacky, but it means it can work when Airflow itself is in the same Docker
            # engine where this task is going to run (unlike say trying to mount a file in)
            self.environment["__PYTHON_SCRIPT"] = _b64_encode_file(script_filename)
            if self.op_args or self.op_kwargs:
                self.environment["__PYTHON_INPUT"] = _b64_encode_file(input_filename)
            else:
                self.environment["__PYTHON_INPUT"] = ""

            self.command = (
                f"""bash -cx  '{_generate_decode_command("__PYTHON_SCRIPT", "/tmp/script.py")} &&"""
                f'{_generate_decode_command("__PYTHON_INPUT", "/tmp/script.in")} &&'
                f'python /tmp/script.py /tmp/script.in /tmp/script.out\''
            )
            return super().execute(context)

    def _get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, "@task.docker")
        return res


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def docker_task(
    python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
):
    """
    Python operator decorator. Wraps a function into an Airflow operator.
    Also accepts any argument that DockerOperator will via ``kwargs``. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :type python_callable: Optional[Callable]
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_DockerDecoratedOperator,
        **kwargs,
    )


# [START decoratormixin]
class DockerDecoratorMixin:
    """
    Helper class for inheritance. This class is only used during type checking or auto-completion

    :meta private:
    """

    def docker(
        self,
        multiple_outputs: Optional[bool] = None,
        use_dill: bool = False,
        image: str = "",
        api_version: Optional[str] = None,
        container_name: Optional[str] = None,
        cpus: float = 1.0,
        docker_url: str = 'unix://var/run/docker.sock',
        environment: Optional[Dict] = None,
        private_environment: Optional[Dict] = None,
        force_pull: bool = False,
        mem_limit: Optional[Union[float, str]] = None,
        host_tmp_dir: Optional[str] = None,
        network_mode: Optional[str] = None,
        tls_ca_cert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_hostname: Optional[Union[str, bool]] = None,
        tls_ssl_version: Optional[str] = None,
        tmp_dir: str = '/tmp/airflow',
        user: Optional[Union[str, int]] = None,
        mounts: Optional[List[str]] = None,
        working_dir: Optional[str] = None,
        xcom_all: bool = False,
        docker_conn_id: Optional[str] = None,
        dns: Optional[List[str]] = None,
        dns_search: Optional[List[str]] = None,
        auto_remove: bool = False,
        shm_size: Optional[int] = None,
        tty: bool = False,
        privileged: bool = False,
        cap_add: Optional[Iterable[str]] = None,
        extra_hosts: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """
        :param python_callable: A python function with no references to outside variables,
            defined with def, which will be run in a virtualenv
        :type python_callable: function
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        :param use_dill: Whether to use dill or pickle for serialization
        :type use_dill: bool
        :param image: Docker image from which to create the container.
            If image tag is omitted, "latest" will be used.
        :type image: str
        :param api_version: Remote API version. Set to ``auto`` to automatically
            detect the server's version.
        :type api_version: str
        :param container_name: Name of the container. Optional (templated)
        :type container_name: str or None
        :param cpus: Number of CPUs to assign to the container.
            This value gets multiplied with 1024. See
            https://docs.docker.com/engine/reference/run/#cpu-share-constraint
        :type cpus: float
        :param docker_url: URL of the host running the docker daemon.
            Default is unix://var/run/docker.sock
        :type docker_url: str
        :param environment: Environment variables to set in the container. (templated)
        :type environment: dict
        :param private_environment: Private environment variables to set in the container.
            These are not templated, and hidden from the website.
        :type private_environment: dict
        :param force_pull: Pull the docker image on every run. Default is False.
        :type force_pull: bool
        :param mem_limit: Maximum amount of memory the container can use.
            Either a float value, which represents the limit in bytes,
            or a string like ``128m`` or ``1g``.
        :type mem_limit: float or str
        :param host_tmp_dir: Specify the location of the temporary directory on the host which will
            be mapped to tmp_dir. If not provided defaults to using the standard system temp directory.
        :type host_tmp_dir: str
        :param network_mode: Network mode for the container.
        :type network_mode: str
        :param tls_ca_cert: Path to a PEM-encoded certificate authority
            to secure the docker connection.
        :type tls_ca_cert: str
        :param tls_client_cert: Path to the PEM-encoded certificate
            used to authenticate docker client.
        :type tls_client_cert: str
        :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
        :type tls_client_key: str
        :param tls_hostname: Hostname to match against
            the docker server certificate or False to disable the check.
        :type tls_hostname: str or bool
        :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
        :type tls_ssl_version: str
        :param tmp_dir: Mount point inside the container to
            a temporary directory created on the host by the operator.
            The path is also made available via the environment variable
            ``AIRFLOW_TMP_DIR`` inside the container.
        :type tmp_dir: str
        :param user: Default user inside the docker container.
        :type user: int or str
        :param mounts: List of mounts to mount into the container, e.g.
            ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
        :type mounts: list
        :param working_dir: Working directory to
            set on the container (equivalent to the -w switch the docker client)
        :type working_dir: str
        :param xcom_all: Push all the stdout or just the last line.
            The default is False (last line).
        :type xcom_all: bool
        :param docker_conn_id: ID of the Airflow connection to use
        :type docker_conn_id: str
        :param dns: Docker custom DNS servers
        :type dns: list[str]
        :param dns_search: Docker custom DNS search domain
        :type dns_search: list[str]
        :param auto_remove: Auto-removal of the container on daemon side when the
            container's process exits.
            The default is False.
        :type auto_remove: bool
        :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
            greater than 0. If omitted uses system default.
        :type shm_size: int
        :param tty: Allocate pseudo-TTY to the container
            This needs to be set see logs of the Docker container.
        :type tty: bool
        :param privileged: Give extended privileges to this container.
        :type privileged: bool
        :param cap_add: Include container capabilities
        :type cap_add: list[str]
        """
        ...
        # [END decoratormixin]
