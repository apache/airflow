# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from builtins import bytes
import os
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars

import rpy2.robjects as robjects
from rpy2.rinterface import RRuntimeError


class ROperator(BaseOperator):
    """
    Execute an R script or command

    If BaseOperator.do_xcom_push is True, the last line written to stdout
    will also be pushed to an XCom when the R command completes

    :param r_command: The command or a reference to an R script (must have
        '.r' extension) to be executed (templated)
    :type r_command: string
    :param env: Optional list of environment variables and their (string)
        values to set (templated). Unlike `BashOperator`, this does not
        replace the current environment, although it can be used to override
        existing values. Values can be read in R with `Sys.getenv()`.
    :type env: dict
    :param output_encoding: encoding output from R (default: 'utf-8')
    :type output_encoding: string

    """

    template_fields = ('r_command', 'env',)
    template_ext = ('.r', '.R')
    ui_color = '#C8D5E6'

    @apply_defaults
    def __init__(
            self,
            r_command,
            env={},
            output_encoding='utf-8',
            *args, **kwargs):

        super(ROperator, self).__init__(*args, **kwargs)
        self.r_command = r_command
        self.env = env
        self.output_encoding = output_encoding

    def execute(self, context):
        """
        Execute the R command or script in a temporary directory
        """

        # Export additional environment variables
        os.environ.update(self.env)

        # Export context as environment variables
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.info('Exporting the following env vars:\n%s',
                      '\n'.join(["{}={}".format(k, v)
                                 for k, v in
                                 airflow_context_vars.items()]))
        os.environ.update(airflow_context_vars)

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(self.r_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)

                self.log.info("Temporary script location: %s", script_location)
                self.log.info("Running command(s):\n%s", self.r_command)

                try:
                    res = robjects.r.source(fname, echo=False)
                except RRuntimeError as e:
                    self.log.error("Received R error: %s", e)
                    res = None

                # This will be a pickled rpy2.robjects.vectors.ListVector
                return res
