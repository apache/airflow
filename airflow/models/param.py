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
from typing import Any, Dict, Optional

import jsonschema
from jsonschema import FormatChecker
from jsonschema.exceptions import ValidationError

from airflow.exceptions import AirflowException
from airflow.utils.context import Context


class NoValueSentinel:
    """Sentinel class used to distinguish between None and no passed value"""

    def __str__(self):
        return "NoValueSentinel"

    def __repr__(self):
        return "NoValueSentinel"


class Param:
    """
    Class to hold the default value of a Param and rule set to do the validations. Without the rule set
    it always validates and returns the default value.

    :param default: The value this Param object holds
    :type default: Any
    :param description: Optional help text for the Param
    :type description: str
    :param schema: The validation schema of the Param, if not given then all kwargs except
        default & description will form the schema
    :type schema: dict
    """

    __NO_VALUE_SENTINEL = NoValueSentinel()
    CLASS_IDENTIFIER = '__class'

    def __init__(self, default: Any = __NO_VALUE_SENTINEL, description: str = None, **kwargs):
        self.value = default
        self.description = description
        self.schema = kwargs.pop('schema') if 'schema' in kwargs else kwargs

        # If we have a value, validate it once. May raise ValueError.
        if self.has_value:
            try:
                jsonschema.validate(self.value, self.schema, format_checker=FormatChecker())
            except ValidationError as err:
                raise ValueError(err)

    def resolve(self, value: Optional[Any] = __NO_VALUE_SENTINEL, suppress_exception: bool = False) -> Any:
        """
        Runs the validations and returns the Param's final value.
        May raise ValueError on failed validations, or TypeError
        if no value is passed and no value already exists.

        :param value: The value to be updated for the Param
        :type value: Optional[Any]
        :param suppress_exception: To raise an exception or not when the validations fails.
            If true and validations fails, the return value would be None.
        :type suppress_exception: bool
        """
        final_val = value if value != self.__NO_VALUE_SENTINEL else self.value
        if isinstance(final_val, NoValueSentinel):
            if suppress_exception:
                return None
            raise TypeError("No value passed and Param has no default value")
        try:
            jsonschema.validate(final_val, self.schema, format_checker=FormatChecker())
        except ValidationError as err:
            if suppress_exception:
                return None
            raise ValueError(err) from None
        self.value = final_val
        return final_val

    def dump(self) -> dict:
        """Dump the Param as a dictionary"""
        out_dict = {self.CLASS_IDENTIFIER: f'{self.__module__}.{self.__class__.__name__}'}
        out_dict.update(self.__dict__)
        return out_dict

    @property
    def has_value(self) -> bool:
        return not isinstance(self.value, NoValueSentinel)


class ParamsDict(dict):
    """
    Class to hold all params for dags or tasks. All the keys are strictly string and values
    are converted into Param's object if they are not already. This class is to replace param's
    dictionary implicitly and ideally not needed to be used directly.
    """

    def __init__(self, dict_obj: Optional[Dict] = None, suppress_exception: bool = False):
        """
        Init override for ParamsDict
        :param dict_obj: A dict or dict like object to init ParamsDict
        :type dict_obj: Optional[dict]
        :param suppress_exception: Flag to suppress value exceptions while initializing the ParamsDict
        :type suppress_exception: bool
        """
        params_dict = {}
        dict_obj = dict_obj or {}
        for k, v in dict_obj.items():
            if not isinstance(v, Param):
                params_dict[k] = Param(v)
            else:
                params_dict[k] = v
        super().__init__(params_dict)
        self.suppress_exception = suppress_exception

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Override for dictionary's ``setitem`` method. This method make sure that all values are of
        Param's type only.

        :param key: A key which needs to be inserted or updated in the dict
        :type key: str
        :param value: A value which needs to be set against the key. It could be of any
            type but will be converted and stored as a Param object eventually.
        :type value: Any
        """
        if isinstance(value, Param):
            param = value
        elif key in self:
            param = dict.__getitem__(self, key)
            param.resolve(value=value, suppress_exception=self.suppress_exception)
        else:
            # if the key isn't there already and if the value isn't of Param type create a new Param object
            param = Param(value)

        super().__setitem__(key, param)

    def __getitem__(self, key: str) -> Any:
        """
        Override for dictionary's ``getitem`` method. After fetching the key, it would call the
        resolve method as well on the Param object.

        :param key: The key to fetch
        :type key: str
        """
        param = super().__getitem__(key)
        return param.resolve(suppress_exception=self.suppress_exception)

    def dump(self) -> dict:
        """Dumps the ParamsDict object as a dictionary, while suppressing exceptions"""
        return {k: v.resolve(suppress_exception=True) for k, v in self.items()}

    def update(self, other_dict: dict) -> None:
        """
        Override for dictionary's update method.
        :param other_dict: A dict type object which needs to be merged in the ParamsDict object
        :type other_dict: dict
        """
        try:
            for k, v in other_dict.items():
                self.__setitem__(k, v)
        except ValueError as ve:
            raise ValueError(f'Invalid input for param {k}: {ve}') from None

    def validate(self) -> dict:
        """Validates & returns all the Params object stored in the dictionary"""
        resolved_dict = {}
        try:
            for k, v in dict.items(self):
                resolved_dict[k] = v.resolve(suppress_exception=self.suppress_exception)
        except ValueError as ve:
            raise ValueError(f'Invalid input for param {k}: {ve}') from None

        return resolved_dict


class DagParam:
    """
    Class that represents a DAG run parameter & binds a simple Param object to a name within a DAG instance,
    so that it can be resolved during the run time via ``{{ context }}`` dictionary. The ideal use case of
    this class is to implicitly convert args passed to a method which is being decorated by ``@dag`` keyword.

    It can be used to parameterize your dags. You can overwrite its value by setting it on conf
    when you trigger your DagRun.

    This can also be used in templates by accessing ``{{context.params}}`` dictionary.

    **Example**:

        with DAG(...) as dag:
          EmailOperator(subject=dag.param('subject', 'Hi from Airflow!'))

    :param current_dag: Dag being used for parameter.
    :type current_dag: airflow.models.DAG
    :param name: key value which is used to set the parameter
    :type name: str
    :param default: Default value used if no parameter was set.
    :type default: Any
    """

    def __init__(self, current_dag, name: str, default: Optional[Any] = None):
        if default:
            current_dag.params[name] = default
        self._name = name
        self._default = default

    def resolve(self, context: Context) -> Any:
        """Pull DagParam value from DagRun context. This method is run during ``op.execute()``."""
        default = self._default
        if not self._default:
            default = context['params'][self._name] if self._name in context['params'] else None
        resolved = context['dag_run'].conf.get(self._name, default)
        if not resolved:
            raise AirflowException(f'No value could be resolved for parameter {self._name}')
        return resolved
