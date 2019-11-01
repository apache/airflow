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
""""
Deserializer list or dict to model of Kubernetes Python API Client.

Implementation based on the Kubernetes Python Client:
https://github.com/kubernetes-client/python/blob/v10.0.1/kubernetes/client/api_client.py#L614

I copied it, because these functions are part of the private interface. :-(
"""

import re
from datetime import date, datetime

from kubernetes.client import ApiClient, models
from kubernetes.client.rest import ApiException


def __deserialize_primitive(data, klass):
    """
    Deserializes string to primitive type.

    :param data: str.
    :param klass: class literal.
    :return: int, long, float, str, bool.
    """
    try:
        return klass(data)
    except TypeError:
        return data


def __deserialize_object(value):
    """
    Return a original value.

    :return: object.
    """
    return value


def __deserialize_date(string):
    """
    Deserializes string to date.

    :param string: str.
    :return: date.
    """
    try:
        from dateutil.parser import parse

        return parse(string).date()
    except ImportError:
        return string
    except ValueError:
        raise ApiException(status=0, reason="Failed to parse `{0}` into a date object".format(string))


def __deserialize_datatime(string):
    """
    Deserializes string to datetime.
    The string should be in iso8601 datetime format.

    :param string: str.
    :return: datetime.
    """
    try:
        from dateutil.parser import parse

        return parse(string)
    except ImportError:
        return string
    except ValueError:
        raise ApiException(status=0, reason=("Failed to parse `{0}` into a datetime object".format(string)))


def __deserialize_model(data, klass):
    """
    Deserializes list or dict to model.

    :param data: dict, list.
    :param klass: class literal.
    :return: model object.
    """

    if not klass.swagger_types and not hasattr(klass, "get_real_child_model"):
        return data

    kwargs = {}
    if klass.swagger_types is not None:
        for attr, attr_type in klass.swagger_types.items():
            if data is not None and klass.attribute_map[attr] in data and isinstance(data, (list, dict)):
                value = data[klass.attribute_map[attr]]
                kwargs[attr] = deserialize(value, attr_type)

    instance = klass(**kwargs)

    if hasattr(instance, "get_real_child_model"):
        klass_name = instance.get_real_child_model(data)
        if klass_name:
            instance = deserialize(data, klass_name)
    return instance


def deserialize(data, klass):  # pylint: disable=too-many-return-statements
    """
    Deserializes dict, list, str into an object.

    :param data: dict, list or str.
    :param klass: class literal, or string of class name.
    :return: object.
    """
    if data is None:
        return None

    if isinstance(klass, str):
        if klass.startswith("list["):
            sub_kls = re.match(r"list\[(.*)\]", klass).group(1)
            return [deserialize(sub_data, sub_kls) for sub_data in data]

        if klass.startswith("dict("):
            sub_kls = re.match(r"dict\(([^,]*), (.*)\)", klass).group(2)
            return {k: deserialize(v, sub_kls) for k, v in data.items()}

        # convert str to class
        if klass in ApiClient.NATIVE_TYPES_MAPPING:
            klass = ApiClient.NATIVE_TYPES_MAPPING[klass]
        else:
            klass = getattr(models, klass)

    if klass in ApiClient.PRIMITIVE_TYPES:
        return __deserialize_primitive(data, klass)
    elif klass == object:
        return __deserialize_object(data)
    elif klass == date:
        return __deserialize_date(data)
    elif klass == datetime:
        return __deserialize_datatime(data)
    else:
        return __deserialize_model(data, klass)
