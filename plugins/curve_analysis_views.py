# -*- coding: utf-8 -*-

from plugins.tightening_controller_views import TighteningControllerView
from flask_babel import lazy_gettext, gettext

from airflow.plugins_manager import AirflowPlugin


class CurveAnalysisControllerView(TighteningControllerView):
    route_base = '/curves_analysis_controller'

    list_title = lazy_gettext("Analysis Via Controller")

    # list_columns = ['controller_name']

    base_permissions = ['can_show', 'can_list']


curve_ana_controller_view = CurveAnalysisControllerView()
curve_ana_controller_package = {"name": gettext("Analysis Via Controller"),
                                "category": gettext("Analysis"),
                                "view": curve_ana_controller_view}


class CurveAnalysisControllerViewPlugin(AirflowPlugin):
    name = "curve_analysis_controller_view"
    appbuilder_views = [curve_ana_controller_package]
