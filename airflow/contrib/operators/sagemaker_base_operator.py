# -*- coding: utf-8 -*-
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

import json
from typing import Iterable

from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SageMakerBaseOperator(BaseOperator):
    """
    This is the base operator for all SageMaker operators.

    :param config: The configuration necessary to start a training job (templated)
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    """

    template_fields = ['config']
    template_ext = ()
    ui_color = '#ededed'

    integer_fields = []  # type: Iterable[Iterable[str]]

    @apply_defaults
    def __init__(self,
                 config,
                 aws_conn_id='aws_default',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config
        self.hook = None

    def parse_integer(self, config, field):
        if len(field) == 1:
            if isinstance(config, list):
                for sub_config in config:
                    self.parse_integer(sub_config, field)
                return
            head = field[0]
            if head in config:
                config[head] = int(config[head])
            return

        if isinstance(config, list):
            for sub_config in config:
                self.parse_integer(sub_config, field)
            return

        head, tail = field[0], field[1:]
        if head in config:
            self.parse_integer(config[head], tail)
        return

    def parse_config_integers(self):
        # Parse the integer fields of training config to integers
        # in case the config is rendered by Jinja and all fields are str
        for field in self.integer_fields:
            self.parse_integer(self.config, field)

    def expand_role(self):
        pass

    def preprocess_config(self):
        self.log.info(
            'Preprocessing the config and doing required s3_operations'
        )
        self.hook = SageMakerHook(aws_conn_id=self.aws_conn_id)

        self.hook.configure_s3_resources(self.config)
        self.parse_config_integers()
        self.expand_role()

        self.log.info(
            'After preprocessing the config is:\n {}'.format(
                json.dumps(self.config, sort_keys=True, indent=4, separators=(',', ': ')))
        )

    def execute(self, context):
        raise NotImplementedError('Please implement execute() in sub class!')
