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

try:
    import ibmmq  # real one might exist
except ModuleNotFoundError:
    import sys
    from unittest.mock import MagicMock

    class MQMIError(Exception):
        def __init__(self, msg="", reason=None):
            super().__init__(msg)
            self.reason = reason

    fake_ibmmq = MagicMock()
    fake_ibmmq.CMQC.MQRC_NO_MSG_AVAILABLE = 2033
    fake_ibmmq.CMQC.MQRC_CONNECTION_BROKEN = 2009
    fake_ibmmq.CMQC.MQGMO_WAIT = 1
    fake_ibmmq.CMQC.MQGMO_NO_SYNCPOINT = 2
    fake_ibmmq.CMQC.MQGMO_CONVERT = 4
    fake_ibmmq.MQMIError = MQMIError
    fake_ibmmq.OD = MagicMock()
    fake_ibmmq.MD = MagicMock()
    fake_ibmmq.GMO = MagicMock()
    fake_ibmmq.CSP = MagicMock()
    fake_ibmmq.Queue = MagicMock()
    fake_ibmmq.connect = MagicMock()
    fake_ibmmq.RFH2 = MagicMock()

    sys.modules["ibmmq"] = fake_ibmmq
