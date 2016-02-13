from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
from flask.ext.admin.form import DateTimePickerWidget
from wtforms import DateTimeField, SelectField
from flask_wtf import Form


class DateTimeForm(Form):
    # Date filter form needed for gantt and graph view
    execution_date = DateTimeField(
        "Execution date", widget=DateTimePickerWidget())


class TreeForm(Form):
    base_date = DateTimeField(
        "Anchor date", widget=DateTimePickerWidget(), default=datetime.now())
    num_runs = SelectField("Number of runs", default=25, choices=(
        (5, "5"),
        (25, "25"),
        (50, "50"),
        (100, "100"),
        (365, "365"),
    ))

