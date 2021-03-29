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

from marshmallow import fields
from marshmallow.schema import Schema


class OAUTHSchema(Schema):
    """A little information needed for UI"""

    name = fields.String()
    icon = fields.String()


class OPENIDSchema(Schema):
    """A little information needed for the UI"""

    name = fields.String()
    url = fields.String()


class InfoSchema(Schema):
    """Authentication Info Schema"""

    auth_type = fields.String()
    oauth_providers = fields.List(fields.Nested(OAUTHSchema))
    openid_providers = fields.List(fields.Nested(OPENIDSchema))


class LoginForm(Schema):
    """Used to load credentials"""

    username = fields.String(required=True)
    password = fields.String(required=True)


info_schema = InfoSchema()
login_form_schema = LoginForm()
