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

import inspect
import os
import pickle
import tempfile
from textwrap import dedent
from typing import TYPE_CHECKING, Callable, Optional, Sequence, TypeVar

import dill
from kubernetes.client import models as k8s

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.python_virtualenv import remove_task_decorator, write_python_script

if TYPE_CHECKING:
    from airflow.utils.context import Context


def _generate_decode_command(env_var, file):
    # We don't need `f.close()` as the interpreter is about to exit anyway
    return (
        f'python -c "import base64, os;'
        # rf'x = base64.b64decode(os.environ[\"{env_var}\"]);'
        # r
        rf'x = os.environ[\"{env_var}\"];'
        # r
        rf'f = open(\"{file}\", \"w\"); f.write(x);"'
    )


def _b64_encode_file(filename):
    with open(filename) as file_to_encode:
        # return base64.b64encode(file_to_encode.read())
        return file_to_encode.read()


class _KubernetesDecoratedOperator(DecoratedOperator, KubernetesPodOperator):
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

    template_fields: Sequence[str] = ('op_args', 'op_kwargs')

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ('python_callable',)

    def __init__(
        self,
        use_dill=False,
        **kwargs,
    ) -> None:
        print("KUBERNETES EXECUTOR" + str(kwargs))
        command = "dummy command"
        self.pickling_library = dill if use_dill else pickle

        # Image, name and namespace are all required.
        if not 'image' in kwargs:
            kwargs['image'] = 'python'

        if not 'name' in kwargs:
            kwargs['name'] = 'test'

        if not 'namespace' in kwargs:
            kwargs['namespace'] = 'default'

        super().__init__(**kwargs)

    def write_python_script(
        jinja_context: dict,
        filename: str,
        render_template_as_native_obj: bool = False,
    ):
        """
        Renders the python script to a file to execute in the virtual environment.

        :param jinja_context: The jinja context variables to unpack and replace with its placeholders in the
            template file.
        :type jinja_context: dict
        :param filename: The name of the file to dump the rendered script to.
        :type filename: str
        :param render_template_as_native_obj: If ``True``, rendered Jinja template would be converted
            to a native Python object
        """
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(__file__))
        template_env: jinja2.Environment
        if render_template_as_native_obj:
            template_env = jinja2.nativetypes.NativeEnvironment(
                loader=template_loader, undefined=jinja2.StrictUndefined
            )
        else:
            template_env = jinja2.Environment(loader=template_loader, undefined=jinja2.StrictUndefined)
        template = template_env.get_template('python_virtualenv_script.jinja2')

        template.stream(**jinja_context).dump(filename)

    def execute(self, context: 'Context'):

        tmp_dir = tempfile.mkdtemp(prefix='venv')
        # tempfile.mkdtemp()
        input_filename = os.path.join(tmp_dir, 'script.in')
        script_filename = os.path.join(tmp_dir, 'script.py')

        with open(input_filename, 'wb') as file:
            if self.op_args or self.op_kwargs:
                self.pickling_library.dump({'args': self.op_args, 'kwargs': self.op_kwargs}, file)
        py_source = self._get_python_source()
        print("KUBERNETES DECORATOR" + py_source)

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

        # Pass the python script to be executed, and the input args, via environment variables
        # and its injected to the pod as a config map
        # self.environment["__PYTHON_SCRIPT"] = _b64_encode_file(script_filename)
        # if self.op_args or self.op_kwargs:
        #     self.environment["__PYTHON_INPUT"] = _b64_encode_file(input_filename)
        # else:
        #     self.environment["__PYTHON_INPUT"] = ""
        #
        # self.command = (
        #     f"""bash -cx  '{_generate_decode_command("__PYTHON_SCRIPT", "/tmp/script.py")} &&"""
        #     f'{_generate_decode_command("__PYTHON_INPUT", "/tmp/script.in")} &&'
        #     f'python /tmp/script.py /tmp/script.in /tmp/script.out\''
        # )

        # volumeMounts:
        # - mountPath: / singlefile / hosts
        # name: etc
        # subPath: hosts
        #
        #
        # volumes:
        # - name: etc
        # hostPath:
        # path: / etc

        # directory_prefix = script_filename.rsplit('/',1)[0]
        #
        # #directory_prefix = '/tmp/venv8upj253q' + '/'
        # file_name = script_filename.rsplit('/',1)[1]
        #
        #
        # #script_filename='/tmp_node/venv8upj253q/script.py'
        # self.volumes.append(k8s.V1Volume(name='script-2', host_path=
        # k8s.V1HostPathVolumeSource(path=script_filename, type='File')))
        # self.volume_mounts.append(k8s.V1VolumeMount(mount_path=script_filename, name='script-2', read_only=True))
        #                                             #sub_path=file_name))

        # Pass the python script to be executed, and the input args, via environment variables
        # and its injected to the pod as a config map
        self.environment = {}
        self.environment["__PYTHON_SCRIPT"] = _b64_encode_file(script_filename)

        print("ENVIRONMENT" + str(self.environment))
        # if self.op_args or self.op_kwargs:
        #     self.environment["__PYTHON_INPUT"] = _b64_encode_file(input_filename)
        # else:
        #     self.environment["__PYTHON_INPUT"] = ""

        self.env_vars.append(k8s.V1EnvVar(name="__PYTHON_SCRIPT", value=_b64_encode_file(script_filename)))

        self.cmds.append("bash")
        self.arguments.append("-cx")
        argument_string = (
            f'{_generate_decode_command("__PYTHON_SCRIPT", "/tmp/script.py")} && python /tmp/script.py'
        )
        print("ARGUMENT" + argument_string)

        self.arguments.append(
            f'{_generate_decode_command("__PYTHON_SCRIPT", "/tmp/script.py")} && python /tmp/script.py && sleep 1000'
        )

        print("COMMAND" + str(self.cmds))

        print("ARGUMENTS" + str(self.arguments))

        # self.command = f"python {script_filename}"
        # self.command = f"ls /tmp"
        # self.command = "python"

        # self.cmds.append(self.command)
        # self.arguments.append(script_filename)
        # self.arguments.append(script_filename)
        # self.arguments.append(script_filename)
        # self.arguments.append("/tmp/script.in")
        # self.arguments.append("/tmp/script.out")

        # volumes:
        # - name: config - volume
        #     configMap:
        #     name: sherlock - config

        # cmap = k8s.V1ConfigMap()
        # cmap.data = {}
        # cmap.data['script'] = script_filename
        #
        # vol_source = k8s.V1ConfigMapVolumeSource(name='script')
        #
        # self.volumes.append(k8s.V1Volume(name='config-volume', config_map=vol_source))
        #
        # self.env_from.append(k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='script')))

        # print("KUBERNETES COMMAND" + self.command)
        return super().execute(context)

    def _get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, "@task.kubernetes")
        return res


T = TypeVar("T", bound=Callable)


def kubernetes_task(
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
        decorated_operator_class=_KubernetesDecoratedOperator,
        **kwargs,
    )
