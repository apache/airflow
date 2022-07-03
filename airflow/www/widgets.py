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
from __future__ import annotations

from flask_appbuilder.fieldwidgets import BS3TextAreaFieldWidget, BS3TextFieldWidget
from flask_appbuilder.widgets import RenderTemplateWidget
from markupsafe import Markup
from wtforms.widgets import html_params


class AirflowModelListWidget(RenderTemplateWidget):
    """Airflow model list"""

    template = 'airflow/model_list.html'


class AirflowDateTimePickerWidget:
    """Airflow date time picker widget"""

    data_template = (
        '<div class="input-group datetime datetimepicker">'
        '<span class="input-group-addon"><span class="material-icons cursor-hand">calendar_today</span>'
        "</span>"
        '<input class="form-control" %(text)s />'
        "</div>"
    )

    def __call__(self, field, **kwargs):
        kwargs.setdefault("id", field.id)
        kwargs.setdefault("name", field.name)
        if not field.data:
            field.data = ''
        template = self.data_template

        return Markup(
            template % {"text": html_params(type="text", value=field.data, required=True, **kwargs)}
        )


class AirflowDateTimePickerROWidget(AirflowDateTimePickerWidget):
    """Airflow Read-only date time picker widget"""

    def __call__(self, field, **kwargs):
        kwargs['readonly'] = 'true'
        return super().__call__(field, **kwargs)


class BS3TextFieldROWidget(BS3TextFieldWidget):
    """Read-only single-line text input Widget (BS3TextFieldWidget)"""

    def __call__(self, field, **kwargs):
        kwargs['readonly'] = 'true'
        return super().__call__(field, **kwargs)


class BS3TextAreaROWidget(BS3TextAreaFieldWidget):
    """Read-only multi-line text area Widget (BS3TextAreaROWidget)"""

    def __call__(self, field, **kwargs):
        kwargs['readonly'] = 'true'
        return super().__call__(field, **kwargs)


class AirflowVariableShowWidget(RenderTemplateWidget):
    """Airflow variable show widget"""

    template = 'airflow/variable_show_widget.html'
