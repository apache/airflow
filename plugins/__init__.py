# -*- coding: utf-8 -*-

from airflow.configuration import conf
from flask_appbuilder import ModelView
from airflow.www.widgets import AirflowModelListWidget
from airflow.www import utils as wwwutils


PAGE_SIZE = conf.getint('webserver', 'page_size')


class CurveAnalysisListWidget(AirflowModelListWidget):
    template = 'curve_analysis_list.html'


class AirflowModelView(ModelView):
    list_widget = AirflowModelListWidget
    page_size = PAGE_SIZE

    CustomSQLAInterface = wwwutils.CustomSQLAInterface
