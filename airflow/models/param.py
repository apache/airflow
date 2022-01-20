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
import copy
import json
import warnings
from typing import Any, Dict, ItemsView, MutableMapping, Optional, ValuesView

import jsonschema
from jsonschema import FormatChecker
from jsonschema.exceptions import ValidationError

from airflow.exceptions import AirflowException, ParamValidationError
from airflow.utils.context import Context
from airflow.utils.types import NOTSET, ArgNotSet


class Param:
    """
    Class to hold the default value of a Param and rule set to do the validations. Without the rule set
    it always validates and returns the default value.

    :param default: The value this Param object holds
    :param description: Optional help text for the Param
    :param schema: The validation schema of the Param, if not given then all kwargs except
        default & description will form the schema
    """

    CLASS_IDENTIFIER = '__class'

    def __init__(self, default: Any = NOTSET, description: Optional[str] = None, **kwargs):
        self.value = default
        self.description = description
        self.schema = kwargs.pop('schema') if 'schema' in kwargs else kwargs

    def __copy__(self) -> "Param":
        return Param(self.value, self.description, schema=self.schema)

    def resolve(self, value: Any = NOTSET, suppress_exception: bool = False) -> Any:
        """
        Runs the validations and returns the Param's final value.
        May raise ValueError on failed validations, or TypeError
        if no value is passed and no value already exists.
        We first check that value is json-serializable; if not, warn.
        In future release we will require the value to be json-serializable.

        :param value: The value to be updated for the Param
        :param suppress_exception: To raise an exception or not when the validations fails.
            If true and validations fails, the return value would be None.
        """
        try:
            json.dumps(value)
        except Exception:
            warnings.warn(
                "The use of non-json-serializable params is deprecated and will be removed in "
                "a future release",
                DeprecationWarning,
            )
        final_val = value if value is not NOTSET else self.value
        if isinstance(final_val, ArgNotSet):
            if suppress_exception:
                return None
            raise ParamValidationError("No value passed and Param has no default value")
        try:
            jsonschema.validate(final_val, self.schema, format_checker=FormatChecker())
        except ValidationError as err:
            if suppress_exception:
                return None
            raise ParamValidationError(err) from None
        self.value = final_val
        return final_val

    def dump(self) -> dict:
        """Dump the Param as a dictionary"""
        out_dict = {self.CLASS_IDENTIFIER: f'{self.__module__}.{self.__class__.__name__}'}
        out_dict.update(self.__dict__)
        return out_dict

    @property
    def has_value(self) -> bool:
        return self.value is not NOTSET


class ParamsDict(MutableMapping[str, Any]):
    """
    Class to hold all params for dags or tasks. All the keys are strictly string and values
    are converted into Param's object if they are not already. This class is to replace param's
    dictionary implicitly and ideally not needed to be used directly.
    """

    __slots__ = ['__dict', 'suppress_exception']

    def __init__(self, dict_obj: Optional[Dict] = None, suppress_exception: bool = False):
        """
        :param dict_obj: A dict or dict like object to init ParamsDict
        :param suppress_exception: Flag to suppress value exceptions while initializing the ParamsDict
        """
        params_dict: Dict[str, Param] = {}
        dict_obj = dict_obj or {}
        for k, v in dict_obj.items():
            if not isinstance(v, Param):
                params_dict[k] = Param(v)
            else:
                params_dict[k] = v
        self.__dict = params_dict
        self.suppress_exception = suppress_exception

    def __copy__(self) -> "ParamsDict":
        return ParamsDict(self.__dict, self.suppress_exception)

    def __deepcopy__(self, memo: Optional[Dict[int, Any]]) -> "ParamsDict":
        return ParamsDict(copy.deepcopy(self.__dict, memo), self.suppress_exception)

    def __contains__(self, o: object) -> bool:
        return o in self.__dict

    def __len__(self) -> int:
        return len(self.__dict)

    def __delitem__(self, v: str) -> None:
        del self.__dict[v]

    def __iter__(self):
        return iter(self.__dict)

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Override for dictionary's ``setitem`` method. This method make sure that all values are of
        Param's type only.

        :param key: A key which needs to be inserted or updated in the dict
        :param value: A value which needs to be set against the key. It could be of any
            type but will be converted and stored as a Param object eventually.
        """
        if isinstance(value, Param):
            param = value
        elif key in self.__dict:
            param = self.__dict[key]
            try:
                param.resolve(value=value, suppress_exception=self.suppress_exception)
            except ParamValidationError as ve:
                raise ParamValidationError(f'Invalid input for param {key}: {ve}') from None
        else:
            # if the key isn't there already and if the value isn't of Param type create a new Param object
            param = Param(value)

        self.__dict[key] = param

    def __getitem__(self, key: str) -> Any:
        """
        Override for dictionary's ``getitem`` method. After fetching the key, it would call the
        resolve method as well on the Param object.

        :param key: The key to fetch
        """
        param = self.__dict[key]
        return param.resolve(suppress_exception=self.suppress_exception)

    def get_param(self, key: str) -> Param:
        """Get the internal :class:`.Param` object for this key"""
        return self.__dict[key]

    def items(self):
        return ItemsView(self.__dict)

    def values(self):
        return ValuesView(self.__dict)

    def update(self, *args, **kwargs) -> None:
        if len(args) == 1 and not kwargs and isinstance(args[0], ParamsDict):
            return super().update(args[0].__dict)
        super().update(*args, **kwargs)

    def dump(self) -> Dict[str, Any]:
        """Dumps the ParamsDict object as a dictionary, while suppressing exceptions"""
        return {k: v.resolve(suppress_exception=True) for k, v in self.items()}

    def validate(self) -> Dict[str, Any]:
        """Validates & returns all the Params object stored in the dictionary"""
        resolved_dict = {}
        try:
            for k, v in self.items():
                resolved_dict[k] = v.resolve(suppress_exception=self.suppress_exception)
        except ParamValidationError as ve:
            raise ParamValidationError(f'Invalid input for param {k}: {ve}') from None

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
    :param name: key value which is used to set the parameter
    :param default: Default value used if no parameter was set.
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
