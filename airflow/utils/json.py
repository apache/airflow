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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime, date
import json
import numpy as np


# Dates and JSON encoding/decoding

def json_ser(obj):
    """json serializer that deals with dates.

    usage: json.dumps(object, default=utils.json.json_ser)
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()


class AirflowJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        # convert dates and numpy objects in a json serializable format
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        elif type(obj) in (np.int_, np.intc, np.intp, np.int8, np.int16,
                           np.int32, np.int64, np.uint8, np.uint16,
                           np.uint32, np.uint64):
            return int(obj)
        elif type(obj) in (np.bool_,):
            return bool(obj)
        elif type(obj) in (np.float_, np.float16, np.float32, np.float64,
                           np.complex_, np.complex64, np.complex128):
            return float(obj)

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)
