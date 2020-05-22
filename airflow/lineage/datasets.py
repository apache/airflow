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
from typing import List

import six
from jinja2 import Environment


def _inherited(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in _inherited(c)]
    )


class DataSet(object):
    attributes = ["name"]  # type: List[str]
    type_name = "DataSet"
    classification_name = "Standard_entities"

    def __init__(self, qualified_name=None, data=None, **kwargs):
        self._qualified_name = qualified_name
        self.context = None
        self._data = dict()

        self._data.update(dict((key, value) for key, value in six.iteritems(kwargs)
                               if key in set(self.attributes)))

        if data:
            if "qualifiedName" in data:
                self._qualified_name = data.pop("qualifiedName")

            self._data = dict((key, value) for key, value in six.iteritems(data)
                              if key in set(self.attributes))

    def set_context(self, context):
        self.context = context

    @property
    def qualified_name(self):
        if self.context:
            env = Environment()
            return env.from_string(self._qualified_name).render(**self.context)

        return self._qualified_name

    def __getattr__(self, attr):
        if attr in self.attributes:
            if self.context:
                env = Environment()
                return env.from_string(self._data.get(attr)).render(**self.context)

            return self._data.get(attr)

        raise AttributeError(attr)

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __iter__(self):
        for key, value in six.iteritems(self._data):
            yield (key, value)

    def as_dict(self):
        attributes = dict(self._data)
        attributes.update({"qualifiedName": self.qualified_name})

        env = Environment()
        if self.context:
            for key, value in six.iteritems(attributes):
                attributes[key] = env.from_string(value).render(**self.context)

        d = {
            "typeName": self.type_name,
            "attributes": attributes,
        }

        return d

    @staticmethod
    def map_type(name):
        for cls in _inherited(DataSet):
            if cls.type_name == name:
                return cls

        raise NotImplementedError("No known mapping for {}".format(name))


class DataBase(DataSet):
    type_name = "dbStore"
    attributes = ["dbStoreType", "storeUse", "source", "description", "userName",
                  "storeUri", "operation", "startTime", "endTime", "commandlineOpts",
                  "attribute_db"]


class File(DataSet):
    type_name = "fs_path"
    attributes = ["name", "path", "isFile", "isSymlink"]

    def __init__(self, name=None, data=None):
        super(File, self).__init__(name=name, data=data)

        self._qualified_name = 'file://' + self.name
        self._data['path'] = self.name


class HadoopFile(File):
    cluster_name = "none"
    attributes = ["name", "path", "clusterName"]

    type_name = "hdfs_file"

    def __init__(self, name=None, data=None):
        super(File, self).__init__(name=name, data=data)

        self._qualified_name = "{}@{}".format(self.name, self.cluster_name)
        self._data['path'] = self.name

        self._data['clusterName'] = self.cluster_name


class StandardAirflowOperator(DataSet):
    """
    Represents airflow operator entity, this type used for storing lineage data
    """

    type_name = "airflow_standard_operator"
    attributes = ["dag_id", "task_id", "task_type", "command", "conn_id", "name", "last_execution_date",
                  "start_date", "end_date", "inputs", "outputs",
                  "template_fields"]

    def __init__(self, additional_operator_attributes=None, **kwargs):
        super(StandardAirflowOperator, self).__init__(**kwargs)
        self.attributes = self.attributes + additional_operator_attributes


class StandardTable(DataSet):
    """
    Represents abstract table atlas entity for any sql-like source
    Table are connected with column using atlas relationship attribute "columns".
    See typedefs in lineage.backend.atlas.typedef
    Relationship attribute "columns" is uses for showing schema in atlas ui.
    """
    type_name = "standard_table"
    attributes = ["name", 'schema_name', "table_name"]


class StandardColumn(DataSet):
    """
    Represent abstract column atlas entity for any sql-like source.
    Column are connected with table using atlas relationship attribute "table".
    See typedefs in lineage.backend.atlas.typedef
    """
    type_name = "standard_column"
    attributes = ['name', 'column', 'type']


class StandardFile(DataSet):
    """
    Represent abstact file in any file system. It can be localfs, hdfs, s3 or gcs.
    """
    type_name = "standard_file"
    attributes = ['name', 'path', 'cluster_name']
