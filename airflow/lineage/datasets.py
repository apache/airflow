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
"""Datasets module."""
import json
from typing import List

from jinja2 import Environment


def _inherited(cls):
    """Return set of subsubclasses of cls."""
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in _inherited(c)]
    )


class DataSet:
    """Define base DataSet class."""
    attributes = []  # type: List[str]
    type_name = "dataSet"

    def __init__(self, qualified_name=None, data=None, **kwargs):
        self._qualified_name = qualified_name
        self.context = None
        self._data = dict()

        self._data.update({key: value for key, value in kwargs.items() if key in set(self.attributes)})

        if data:
            if "qualifiedName" in data:
                self._qualified_name = data.pop("qualifiedName")

            self._data = {key: value for key, value in data.items() if key in set(self.attributes)}

    def set_context(self, context):
        """Set the Dataset context."""
        self.context = context

    @property
    def qualified_name(self):
        """Return the qualified name string."""
        if self.context:
            env = Environment()
            return env.from_string(self._qualified_name).render(**self.context)

        return self._qualified_name

    def __getattr__(self, attr):
        if attr in self.attributes:
            if self.context:
                env = Environment()
                # dump to json here in order to be able to manage dicts and lists
                rendered = env.from_string(
                    json.dumps(self._data.get(attr))
                ).render(**self.context)
                return json.loads(rendered)

            return self._data.get(attr)

        raise AttributeError(attr)

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __iter__(self):
        yield from self._data.items()

    def as_dict(self):
        """
        Return a dictionary containing two elements: typeName and attributes.
        attributes is a nested dictionary of the DataSet attributes
        """
        attributes = dict(self._data)
        attributes.update({"qualifiedName": self.qualified_name})

        env = Environment()
        if self.context:
            for key, value in attributes.items():
                attributes[key] = json.loads(
                    env.from_string(json.dumps(value)).render(**self.context)
                )

        dataset_dict = {
            "typeName": self.type_name,
            "attributes": attributes,
        }

        return dataset_dict

    @staticmethod
    def map_type(name):
        """
        Return a derived class of DataSet if the input string name matches
        the type_name of an inherited class of DataSet.
        Otherwise raise NotImplementedError.
        """
        for cls in _inherited(DataSet):
            if cls.type_name == name:
                return cls

        raise NotImplementedError("No known mapping for {}".format(name))


class DataBase(DataSet):
    """DataBase class, subclass of DataSet."""
    type_name = "dbStore"
    attributes = ["dbStoreType", "storeUse", "source", "description", "userName",
                  "storeUri", "operation", "startTime", "endTime", "commandlineOpts",
                  "attribute_db"]


class File(DataSet):
    """File class, subclass of DataSet."""
    type_name = "fs_path"
    attributes = ["name", "path", "isFile", "isSymlink"]

    def __init__(self, name=None, data=None):
        super().__init__(name=name, data=data)

        self._qualified_name = 'file://' + self.name
        self._data['path'] = self.name


class HadoopFile(File):
    """HadoopFile class, subclass of DataSet."""
    cluster_name = "none"
    attributes = ["name", "path", "clusterName"]

    type_name = "hdfs_file"

    def __init__(self, name=None, data=None):
        super().__init__(name=name, data=data)

        self._qualified_name = "{}@{}".format(self.name, self.cluster_name)
        self._data['path'] = self.name

        self._data['clusterName'] = self.cluster_name


class Operator(DataSet):
    """Operator class, subclass of DataSet."""
    type_name = "airflow_operator"

    # todo we can derive this from the spec
    attributes = ["dag_id", "task_id", "command", "conn_id", "name", "execution_date",
                  "start_date", "end_date", "inputs", "outputs"]
