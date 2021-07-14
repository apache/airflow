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

from typing import Any

from jsonschema import Draft4Validator, FormatChecker
from jsonschema.exceptions import ValidationError


class Param:
    """
    Class to hold the default vallue of a Param and rule set to do the validations

    :param default: The value of this Param object holds
    :type default: Any
    :param description: Optional help text for the Param
    :type description: str
    :param schema: The validation schema of the Param, if not given then all kwargs except
    default & description will form the schema
    :type schema: dict
    """

    def __init__(self, default: Any = None, description: str = None, **kwargs):
        self.default = default
        self.description = description
        self.schema = kwargs.pop('schema') if 'schema' in kwargs else kwargs

        # If default is not None, then validate it once, may raise ValueError
        if default:
            try:
                validator = Draft4Validator(self.schema, format_checker=FormatChecker())
                validator.validate(self.default)
            except ValidationError as err:
                raise ValueError(err)

    def __call__(self, suppress_exception: bool = False) -> Any:
        """
        Runs the validations and returns the Param's default value. May raise ValueError on failed validations.

        :param suppress_exception: To raise an exception or not when the validations fails.
        If true and validations fails, the return value would be None.
        :type suppress_exception: bool
        """
        try:
            validator = Draft4Validator(self.schema, format_checker=FormatChecker())
            validator.validate(self.default)
        except ValidationError as err:
            if suppress_exception:
                return None
            raise ValueError(err)
        return self.default

    def dump(self) -> dict:
        """Dump the Param as a dictionary"""
        out_dict = {'__type': f'{self.__module__}.{self.__class__.__name__}'}
        out_dict.update(self.__dict__)
        return out_dict
