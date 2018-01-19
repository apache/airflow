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

from qds_sdk.qubole import Qubole
from qds_sdk.sensors import FileSensor, PartitionSensor

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class QuboleSensor(BaseSensorOperator):
    """
    Base class for all Qubole Sensors

    :param qubole_conn_id: The qubole connection to run the sensor against
    :type qubole_conn_id: string
    :param data: a JSON object containing payload, whose presence needs to be checked
    :type data: a JSON object

    .. note:: Both ``data`` and ``qubole_conn_id`` fields are template-supported. You can
    also use ``.txt`` files for template driven use cases.
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

        this.log.info('Poking: %s', self.data)

        status = False
        try:
            status = self.sensor_class.check(self.data)
        except Exception as e:
            logging.exception(e)
            status = False

        this.log.info('Status of this Poke: %s', status)

        return status


class QuboleFileSensor(QuboleSensor):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.sensor_class = FileSensor
        super(QuboleFileSensor, self).__init__(*args, **kwargs)


class QubolePartitionSensor(QuboleSensor):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.sensor_class = PartitionSensor
        super(QubolePartitionSensor, self).__init__(*args, **kwargs)
