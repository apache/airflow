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
from airflow.operators.python_operator import PythonOperator
from collections import namedtuple
import logging
import os
import json
import types

PIPELINE_PREFIX = configuration.get('PIPELINE', 'xcom_prefix')
CONTEXT_KEY = 'pipeline'

PipelineIndex = namedtuple('PipelineIndex', ['operator', 'index'])


def json_serialize(data, context):
    return json.dumps(data)


def json_deserialize(data, context):
    return json.loads(data)


class Pipeline(object):

    def __init__(
            self,
            downstream_task,
            downstream_key,
            upstream_task,
            upstream_key=None,
            serialize=None,
            deserialize=None):
        """
        Pipeline objects are used to pass data from an upstream task to a
        downstream task. Airflow operators are not necessarily run in the
        same environment and may not have access to the same resources, so
        Pipelines are responsible for serializing data to ensure it can be
        accessed by any task that needs it.

        A Pipeline object stores the result of an upstream task with an
        optional `upstream_key`. If the task returns an indexable result
        like a tuple or dictionary, the key is used to select an appropriate
        part rather than the entire result.

        The downstream task is passed the result with a specified `key`. The
        result can be accessed in the downstream task's context as
        `context['pipeline'][downstream_key]` or, if the downstream task is a
        PythonOperator, it will also be passed to the python callable as a
        keyword argument.

        ## Arguments

        :param downstream_task: The Operator that receives the Pipeline result
        :type downstream_task: Operator
        :param downstream_key: The key under which the pipelined result will be     stored
        :type downstream_key: str
        :param upstream_task: The Operator that produces the Pipeline result
        :type upstream_task: Operator
        :param upstream_key: If provided, the upstream operator's result is
            indexed by this value prior to being serialized. For example,
            if the source operator returns a dictionary, this could be
            used to select a specific key rather than the entire result.
        :type upstream_key: object
        :param serialize: A function of `(data, context)` used
            to produce a serialized representation of the data. It is expected
            to return a string that can be used to deserialize the data.
        :type serialize: callable
        :param deserialize: A function of
            `(serialized_data, context)` used to deserialize data from the
            stored representation. It should return the deserialized data.
        :type deserialize: callable
        """
        if downstream_task.task_id not in upstream_task.downstream_task_ids:
            upstream_task.set_downstream(downstream_task)
        self.upstream_task = upstream_task
        self.downstream_task = downstream_task
        self.upstream_key = upstream_key
        self.downstream_key = downstream_key
        if serialize is None:
            serialize = json_serialize
        if deserialize is None:
            deserialize = json_deserialize
        self.serialize = serialize
        self.deserialize = deserialize
        self.logger = logging.getLogger(self.__class__.__name__)

        self._set_task_hooks()

    def _serialize(self, data, context):
        """
        Convert data to a serialized representation. The representation is
        stored and later passed to the deserialize method.

        This method calls self.serialize, if set.

        """
        data = self.serialize(data, context)
        return data

    def _deserialize(self, data, context):
        """
        Deserialize data from a representation.

        This method calls self.deserialize, if set.
        """
        if self.deserialize is not None:
            data = self.deserialize(data, context)
        return data

    def clean_up(self, context):
        """
        Method called after the task runs in case any clean up is needed.
        """
        pass

    def generate_unique_id(self, context, extension=None):
        """
        Returns a url-safe unique name that can be used to reference a specific
        Pipeline result.
        """
        pipeline = self.__class__.__name__
        dag_id = context['dag'].dag_id
        task_id = self.upstream_task.task_id
        execution_date = str(context['execution_date'])

        key = os.path.join(pipeline, dag_id, task_id, execution_date)
        if self.upstream_key is not None:
            key = os.path.join(key, str(self.upstream_key).lstrip('/'))
        if extension is not None:
            key += extension
        return key

    def _upstream_post_execute(self, task, context, result):
        """
        Serializes Pipeline data and pushes an XCom containing the result.
        """
        if self.upstream_key is not None:
            result = result[self.upstream_key]

        # if this XCom already exists, don't do unnecessary work
        if XCom.get_many(
                execution_date=context['execution_date'],
                key='{}:{}'.format(
                    PIPELINE_PREFIX, self.generate_unique_id(context)),
                task_ids=self.upstream_task.task_id,
                dag_ids=context['dag'].dag_id):
            return

        serialized_data = self._serialize(result, context)
        if not isinstance(serialized_data, str):
            raise TypeError(
                'Pipeline data must be a serialized as a string '
                '(received {})'.format(type(serialized_data)))

        XCom.set(
            key='{}:{}'.format(
                PIPELINE_PREFIX, self.generate_unique_id(context)),
            value=serialized_data,
            task_id=self.upstream_task.task_id,
            dag_id=context['dag'].dag_id,
            execution_date=context['execution_date'])

    def _downstream_pre_execute(self, task, context):
        """
        Retrieves pipeline data from an XCom and deserializes it
        """
        serialized_data = XCom.get_one(
            key='{}:{}'.format(
                PIPELINE_PREFIX, self.generate_unique_id(context)),
            task_id=self.upstream_task.task_id,
            dag_id=context['dag'].dag_id,
            execution_date=context['execution_date'])

        data = self._deserialize(serialized_data, context)
        context.setdefault(CONTEXT_KEY, {})[self.downstream_key] = data

        if isinstance(task, PythonOperator):
            task.op_kwargs.update({self.downstream_key: data})

    def _downstream_post_execute(self, task, context, result):
        """
        Cleans up pipeline data
        """
        self.clean_up(context)

    def _set_task_hooks(self):
        """
        Modifies tasks to run Pipelines

        Tasks are given a new attribute called "_pipelines", a set of all
        Pipelines referencing that task. If the attribute is being created, the
        Task's pre_execute and post_execute hooks are also modified to call the
        appropriate Pipeline methods. Otherwise, the Pipeline just adds itself
        to the set.
        """
        for p_task in (self.upstream_task, self.downstream_task):
            if hasattr(p_task, '_pipelines'):
                p_task._pipelines.add(self)
            else:
                original_pre_execute = p_task.pre_execute
                original_post_execute = p_task.post_execute

                def pre_execute(task, context):
                    # call original method
                    original_pre_execute(context)

                    # call pipelines
                    for pipeline in task._pipelines:
                        if task is pipeline.downstream_task:
                            pipeline._downstream_pre_execute(task, context)

                def post_execute(task, context, result):
                    # call original method
                    original_post_execute(context, result)

                    # call pipelines
                    for pipeline in task._pipelines:
                        if task is pipeline.upstream_task:
                            pipeline._upstream_post_execute(
                                task, context, result)
                        elif task is pipeline.downstream_task:
                            pipeline._downstream_post_execute(
                                task, context, result)

                p_task.pre_execute = types.MethodType(pre_execute, p_task)
                p_task.post_execute = types.MethodType(post_execute, p_task)

                p_task._pipelines = set([self])


def add_pipelines(
        downstream_task, pipelines, pipeline_class=Pipeline, **pipeline_kwargs):
    r"""
    :param downstream_task: the downstream task
    :type downstream_task: BaseOperator
    :param pipelines: a dictionary of
        {downstream_key: {'task': upstream_task, 'key': upstream_key}}
        pairs. If the 'key' key is omitted, it is assumed to be None
    :type pipelines: dict
    :param pipeline_class: a Pipeline class or function returning a Pipeline
        class
    :type pipeline_class: Pipeline or callable returning a Pipeline
    """

    if not isinstance(pipelines, dict):
        raise TypeError(
            'pipelines should be a dictionary of '
            r"{downstream_key: {'task': upstream_task, 'key': upstream_key}} "
            "pairs (the 'key' key may be omitted).")
    for downstream_key, upstream_dict in pipelines.items():
        upstream_task = upstream_dict['task']
        upstream_key = upstream_dict.get('key', None)
        pipeline_class(
            downstream_key=downstream_key,
            upstream_task=upstream_task,
            downstream_task=downstream_task,
            upstream_key=upstream_key,
            **pipeline_kwargs)
