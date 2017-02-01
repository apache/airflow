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

from airflow import configuration
from airflow.models import XCom
import os

DATAFLOW_KEY = configuration.get('DATAFLOW', 'key_prefix')
NONE_STRING = '__ALL__'

class Dataflow:

    def __init__(self, upstream_task, index=None):
        """
        Dataflow objects are used to pass data from an upstream task to a
        downstream task. Airflow operators are not necessarily run in the
        same environment and may not have access to the same resources, so
        Dataflows are responsible for serializing data to ensure it can be
        accessed by any task that needs it.

        A Dataflow object represents the result of a task and is defined by
        a `upstream_task` and an optional `index`. If the task returns
        an indexable result like a tuple or dictionary, the index is used to
        select an appropriate piece rather than the entire result.

        Dataflow objects are added to downstream tasks with a key. For example:
        ```python
            downstream_task.add_dataflows(result=Dataflow(upstream_task))
        ```
        This means that the result of the `upstream_task` will be available to
        the `downstream_task` under the key `result`. It can be accessed in the
        `downstream_task`'s context as `context['dataflows']['result']` or, if
        the `downstream_task` is a PythonOperator, it will be passed to the
        python callable as a keyword argument.

        ## Mechanics

        Dataflows have three important methods: `serialize()`, `deserialize()`,
        and `clean_up()`.

        `serialize()` is called after the upstream task runs. It is passed the
        upstream result and expected to return a representation of that data
        that can be pickled and stored in the Airflow database. For very small
        data, it is ok to pass the data  directly. However, for performance and
        practical reasons, it is preferable to use the serialize method to store
        data in distributed storage and return a pointer to that data, like a
        URI.

        `deserialize()` is called before the downstream task runs. It is passed
        the serialized data and expected to return the data it represents.

        `clean_up()` is called after the downstream task runs and can be used
        to delete temporary files or perform any maintainence steps.

        ## Arguments

        :param upstream_task: The Operator that produces the Dataflow result
        :type upstream_task: Operator
        :param index: If provided, the source operator's result is indexed
            by this value prior to being passed to the dataflow. For example,
            if the source operator returns a dictionary, the index could be
            used to select a specific key rather than the entire result.
        :type index: object
        """
        self.upstream_task = upstream_task
        self.source_task_id = upstream_task.task_id
        self.index = index

    def serialize(self, data, context):
        """
        Convert data to a serialized representation. The representation is
        stored and later passed to the deserialize method.

        The serialized data will be stored as a pickled Python object and does
        not necessarily have to be a string; however, for performance reasons,
        large data objects should not be stored directly.

        This method should be overridden by Dataflow subclasses.
        """
        return data

    def deserialize(self, serialized_data, context):
        """
        Deserialize data from a representation.

        This method should be overridden by Dataflow subclasses.
        """
        return serialized_data

    def clean_up(self, context):
        """
        Method called after the task runs in case any clean up is needed.
        """
        pass

    def key(self, context, extension=None):
        """
        Returns a url-safe unique name that can be used to reference a specific
        Dataflow item.
        """
        dag_id = context['dag'].dag_id
        task_id = self.source_task_id
        execution_date = str(context['execution_date'])

        key = os.path.join(dag_id, task_id, execution_date)
        if self.index is not None:
            index = str(self.index).replace('/', '_')
            key = os.path.join(key, str(self.index))
        key = os.path.join(key, 'dataflow')
        if extension is not None:
            key += extension
        return key

    def _set_data(self, data, context):

        if self.index is not None:
            data = data[self.index]

        # if this XCom already exists, don't do unnecessary work
        if XCom.get_many(
                execution_date=context['execution_date'],
                key='{}:{}'.format(DATAFLOW_KEY, self.key(context)),
                task_ids=self.source_task_id,
                dag_ids=context['dag'].dag_id,
                count=True):
            return

        serialized_data = self.serialize(data, context)

        XCom.set(
            key='{}:{}'.format(DATAFLOW_KEY, self.key(context)),
            value=serialized_data,
            task_id=self.source_task_id,
            dag_id=context['dag'].dag_id,
            execution_date=context['execution_date'])

    def _get_data(self, context):

        serialized_data = XCom.get_one(
            key='{}:{}'.format(DATAFLOW_KEY, self.key(context)),
            task_id=self.source_task_id,
            dag_id=context['dag'].dag_id,
            execution_date=context['execution_date'])

        return self.deserialize(serialized_data, context)
