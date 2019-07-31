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

from qds_sdk.qubole import Qubole
from qds_sdk.sensors import FileSensor, PartitionSensor

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class QuboleSensor(BaseSensorOperator):
    """
    Base class for all Qubole Sensors
    """

    template_fields = ('data', 'qubole_conn_id')

    template_ext = ('.txt',)

    @apply_defaults
    def __init__(self, data, qubole_conn_id="qubole_default", *args, **kwargs):
        self.data = data
        self.qubole_conn_id = qubole_conn_id

        if 'poke_interval' in kwargs and kwargs['poke_interval'] < 5:
            raise AirflowException("Sorry, poke_interval can't be less than 5 sec for "
                                   "task '{0}' in dag '{1}'."
                                   .format(kwargs['task_id'], kwargs['dag'].dag_id))

        super(QuboleSensor, self).__init__(*args, **kwargs)

    def poke(self, context):

        conn = BaseHook.get_connection(self.qubole_conn_id)
        Qubole.configure(api_token=conn.password, api_url=conn.host)

        self.log.info('Poking: %s', self.data)

        status = False
        try:
            status = self.sensor_class.check(self.data)
        except Exception as e:
            self.log.exception(e)
            status = False

        self.log.info('Status of this Poke: %s', status)

        return status


class QuboleFileSensor(QuboleSensor):
    """
    Wait for a file or folder to be present in cloud storage
    and check for its presence via QDS APIs

    :param qubole_conn_id: Connection id which consists of qds auth_token
    :type qubole_conn_id: str
    :param data: a JSON object containing payload, whose presence needs to be checked
        Check this `example <https://github.com/apache/airflow/blob/master\
        /airflow/contrib/example_dags/example_qubole_sensor.py>`_ for sample payload
        structure.
    :type data: a JSON object

    .. note:: Both ``data`` and ``qubole_conn_id`` fields support templating. You can
        also use ``.txt`` files for template-driven use cases.
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.sensor_class = FileSensor
        super(QuboleFileSensor, self).__init__(*args, **kwargs)


class QubolePartitionSensor(QuboleSensor):
    """
    Wait for a Hive partition to show up in QHS (Qubole Hive Service)
    and check for its presence via QDS APIs

    :param qubole_conn_id: Connection id which consists of qds auth_token
    :type qubole_conn_id: str
    :param data: a JSON object containing payload, whose presence needs to be checked.
        Check this `example <https://github.com/apache/airflow/blob/master\
        /airflow/contrib/example_dags/example_qubole_sensor.py>`_ for sample payload
        structure.
    :type data: a JSON object

    .. note:: Both ``data`` and ``qubole_conn_id`` fields support templating. You can
        also use ``.txt`` files for template-driven use cases.
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.sensor_class = PartitionSensor
        super(QubolePartitionSensor, self).__init__(*args, **kwargs)
