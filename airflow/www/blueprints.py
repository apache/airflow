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
#
from flask import Blueprint, redirect, url_for
from flask_appbuilder import IndexView, expose

routes = Blueprint('routes', __name__)

# Place any Flask Blueprint routes (non Flask-Appbuilder ones) here

# We can't put this into airflow.www.views because then we would have to import the views module
# inside airflow.www.app prior to the appbuilder object being set up.
# This would break other code in views not using the cached_appbuilder() function.


class AirflowIndexView(IndexView):
    """Redirection to /home using IndexView from Flask-Appbuilder."""
    @expose('/')
    def index(self):
        return redirect(url_for('Airflow.index'))
