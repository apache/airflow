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
from flask import (
    url_for, Markup, Blueprint, redirect,
)
import markdown

routes = Blueprint('routes', __name__)


@routes.route('/')
def index():
    return redirect(url_for('admin.index'))


@routes.route('/health')
def health():
    """ We can add an array of tests here to check the server's health """
    content = Markup(markdown.markdown("The server is healthy!"))
    return content
