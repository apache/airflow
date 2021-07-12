# -*- coding: utf-8 -*-

from airflow.configuration import conf
from flask_appbuilder import ModelView
from airflow.www_rbac.widgets import AirflowModelListWidget
from airflow.www_rbac import utils as wwwutils


PAGE_SIZE = conf.getint('webserver', 'page_size')


class CurveAnalysisListWidget(AirflowModelListWidget):
    template = 'airflow/curve_analysis_list.html'


class AirflowModelView(ModelView):
    list_widget = AirflowModelListWidget
    page_size = PAGE_SIZE

    CustomSQLAInterface = wwwutils.CustomSQLAInterface
