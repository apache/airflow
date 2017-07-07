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

from airflow import AirflowException
import logging


class OpContext(object):
    """
        Data model for operation context of a pod operator with hyper parameters. 
        OpContext is able to communicate the context between PodOperators by 
        encapsulating XCom communication
        Note: do not directly modify the upstreams
        Also note: xcom_instance MUST be set before any attribute of this class can be 
        read.
        :param: task_id             The task ID
    """
    _supported_attributes = {'hyper_parameters', 'custom_return_value'}

    def __init__(self, task_id):
        self.task_id = task_id
        self._upstream = []
        self._result = '__not_set__'
        self._data = {}
        self._xcom_instance = None
        self._parent = None

    def __str__(self):
        return 'upstream: [' + \
               ','.join([u.task_id for u in self._upstream]) + ']\n' + \
               'params:' + ','.join(
            [k + '=' + str(self._data[k]) for k in self._data.keys()])

    def __setattr__(self, name, value):
        if name in self._data:
            raise AirflowException('`{}` is already set'.format(name))
        if name not in self._supported_attributes:
            logging.warn(
                '`{}` is not in the supported attribute list for OpContext'.format(name))
        self.get_xcom_instance().xcom_push(key=name, value=value)
        self._data[name] = value

    def __getattr__(self, item):
        if item not in self._supported_attributes:
            logging.warn(
                '`{}` is not in the supported attribute list for OpContext'.format(item))
        if item not in self._data:
            self._data[item] = self.get_xcom_instance().xcom_pull(key=item,
                                                                  task_ids=self.task_id)
        return self._data[item]

    @property
    def result(self):
        if self._result == '__not_set__':
            self._result = self.get_xcom_instance().xcom_pull(task_ids=self.task_id)
        return self._result

    @result.setter
    def result(self, value):
        if self._result != '__not_set__':
            raise AirflowException('`result` is already set')
        self._result = value

    @property
    def upstream(self):
        return self._upstream

    def append_upstream(self, upstream_op_contexes):
        """
        Appends a list of op_contexts to the upstream. It will create new instances and 
        set the task_id.
        All the upstream op_contextes will share the same xcom_instance with this 
        op_context
        :param upstream_op_contexes: List of upstream op_contextes
        """
        for up in upstream_op_contexes:
            op_context = OpContext(up.tak_id)
            op_context._parent = self
            self._upstream.append(op_context)

    def set_xcom_instance(self, xcom_instance):
        """
        Sets the xcom_instance for this op_context and upstreams
        :param xcom_instance: The Airflow TaskInstance for communication through XCom
        :type xcom_instance: airflow.models.TaskInstance
        """
        self._xcom_instance = xcom_instance

    def get_xcom_instance(self):
        if self._xcom_instance is None and self._parent is None:
            raise AirflowException(
                'Trying to access attribtues from OpContext before setting the '
                'xcom_instance')
        return self._xcom_instance or self._parent.get_xcom_instance()
