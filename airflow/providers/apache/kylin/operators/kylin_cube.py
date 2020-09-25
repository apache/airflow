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

import time
from datetime import datetime
from typing import Optional

from kylinpy import kylinpy

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.kylin.hooks.kylin import KylinHook
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class KylinCubeOperator(BaseOperator):
    """
    This operator is used to submit request about kylin build/refresh/merge,
    and can track job status . so users can easier to build kylin job

    For more detail information in
    `Apache Kylin <http://kylin.apache.org/>`_

    :param kylin_conn_id: The connection id as configured in Airflow administration.
    :type kylin_conn_id: str
    :param project: kylin project name, this param will overwrite the project in kylin_conn_id:
    :type project: str
    :param cube: kylin cube name
    :type cube: str
    :param dsn: (dsn , dsn url of kylin connection ,which will overwrite kylin_conn_id.
        for example: kylin://ADMIN:KYLIN@sandbox/learn_kylin?timeout=60&is_debug=1)
    :type dsn: str
    :param command: (kylin command include 'build', 'merge', 'refresh', 'delete',
        'build_streaming', 'merge_streaming', 'refresh_streaming', 'disable', 'enable',
        'purge', 'clone', 'drop'.
        build - use /kylin/api/cubes/{cubeName}/build rest api,and buildType is ‘BUILD’,
        and you should give start_time and end_time
        refresh - use build rest api,and buildType is ‘REFRESH’
        merge - use build rest api,and buildType is ‘MERGE’
        build_streaming - use /kylin/api/cubes/{cubeName}/build2 rest api,and buildType is ‘BUILD’
        and you should give offset_start and offset_end
        refresh_streaming - use build2 rest api,and buildType is ‘REFRESH’
        merge_streaming - use build2 rest api,and buildType is ‘MERGE’
        delete - delete segment, and you should give segment_name value
        disable - disable cube
        enable - enable cube
        purge - purge cube
        clone - clone cube,new cube name is {cube_name}_clone
        drop - drop cube)
    :type command: str
    :param start_time: build segment start time
    :type start_time: Optional[str]
    :param end_time: build segment end time
    :type end_time: Optional[str]
    :param offset_start: streaming build segment start time
    :type offset_start: Optional[str]
    :param offset_end: streaming build segment end time
    :type offset_end: Optional[str]
    :param segment_name: segment name
    :type segment_name: str
    :param is_track_job: (whether to track job status. if value is True,will track job until
        job status is in("FINISHED", "ERROR", "DISCARDED", "KILLED", "SUICIDAL",
        "STOPPED") or timeout)
    :type is_track_job: bool
    :param interval: track job status,default value is 60s
    :type interval: int
    :param timeout: timeout value,default value is 1 day,60 * 60 * 24 s
    :type timeout: int
    :param eager_error_status: (jobs error status,if job status in this list ,this task will be error.
        default value is tuple(["ERROR", "DISCARDED", "KILLED", "SUICIDAL", "STOPPED"]))
    :type eager_error_status: tuple
    """

    template_fields = (
        'project',
        'cube',
        'dsn',
        'command',
        'start_time',
        'end_time',
        'segment_name',
        'offset_start',
        'offset_end',
    )
    ui_color = '#E79C46'
    build_command = {
        'fullbuild',
        'build',
        'merge',
        'refresh',
        'build_streaming',
        'merge_streaming',
        'refresh_streaming',
    }
    jobs_end_status = {"FINISHED", "ERROR", "DISCARDED", "KILLED", "SUICIDAL", "STOPPED"}

    # pylint: disable=too-many-arguments,inconsistent-return-statements
    @apply_defaults
    def __init__(
        self,
        *,
        kylin_conn_id: Optional[str] = 'kylin_default',
        project: Optional[str] = None,
        cube: Optional[str] = None,
        dsn: Optional[str] = None,
        command: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        offset_start: Optional[str] = None,
        offset_end: Optional[str] = None,
        segment_name: Optional[str] = None,
        is_track_job: bool = False,
        interval: int = 60,
        timeout: int = 60 * 60 * 24,
        eager_error_status=("ERROR", "DISCARDED", "KILLED", "SUICIDAL", "STOPPED"),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.kylin_conn_id = kylin_conn_id
        self.project = project
        self.cube = cube
        self.dsn = dsn
        self.command = command
        self.start_time = start_time
        self.end_time = end_time
        self.segment_name = segment_name
        self.offset_start = offset_start
        self.offset_end = offset_end
        self.is_track_job = is_track_job
        self.interval = interval
        self.timeout = timeout
        self.eager_error_status = eager_error_status
        self.jobs_error_status = [stat.upper() for stat in eager_error_status]

    def execute(self, context):

        _hook = KylinHook(kylin_conn_id=self.kylin_conn_id, project=self.project, dsn=self.dsn)

        _support_invoke_command = kylinpy.CubeSource.support_invoke_command
        if self.command.lower() not in _support_invoke_command:
            raise AirflowException(
                'Kylin:Command {} can not match kylin command list {}'.format(
                    self.command, _support_invoke_command
                )
            )

        kylinpy_params = {
            'start': datetime.fromtimestamp(int(self.start_time) / 1000) if self.start_time else None,
            'end': datetime.fromtimestamp(int(self.end_time) / 1000) if self.end_time else None,
            'name': self.segment_name,
            'offset_start': int(self.offset_start) if self.offset_start else None,
            'offset_end': int(self.offset_end) if self.offset_end else None,
        }
        rsp_data = _hook.cube_run(self.cube, self.command.lower(), **kylinpy_params)
        if self.is_track_job and self.command.lower() in self.build_command:
            started_at = timezone.utcnow()
            job_id = rsp_data.get("uuid")
            if job_id is None:
                raise AirflowException("kylin job id is None")
            self.log.info("kylin job id: %s", job_id)

            job_status = None
            while job_status not in self.jobs_end_status:
                if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                    raise AirflowException('kylin job {} timeout'.format(job_id))
                time.sleep(self.interval)

                job_status = _hook.get_job_status(job_id)
                self.log.info('Kylin job status is %s ', job_status)
                if job_status in self.jobs_error_status:
                    raise AirflowException('Kylin job {} status {} is error '.format(job_id, job_status))

        if self.do_xcom_push:
            return rsp_data
