# -*- coding: utf-8 -*-

from plugins.view.tightening_controller_views import TighteningControllerView
from plugins import PAGE_SIZE, AirflowModelView
from airflow.www_rbac.widgets import AirflowModelListWidget
from flask_babel import lazy_gettext, gettext
from airflow.www_rbac import utils as wwwutils
from flask_appbuilder.models.sqla.filters import BaseFilter

from airflow.plugins_manager import AirflowPlugin


class CurveAnalysisListWidget(AirflowModelListWidget):
    template = 'curve_analysis_list.html'


class TrackNoNotNullFilter(BaseFilter):
    def apply(self, query, func):  # noqa
        result = self.model
        ret = query.filter(result.car_code.isnot(None)).distinct(result.car_code).group_by(result)
        return ret


class BoltNoNotNullFilter(BaseFilter):
    def apply(self, query, func):  # noqa
        result = self.model
        return query.filter(result.bolt_number.isnot(None)).distinct(result.bolt_number).group_by(result)


class CurveAnalysisControllerView(TighteningControllerView):
    route_base = '/curves_analysis_controller'

    list_title = lazy_gettext("Analysis Via Controller")

    # list_columns = ['controller_name']

    base_permissions = ['can_show', 'can_list']


class CurveAnalysisTrackNoView(AirflowModelView):
    route_base = '/curves_analysis_track'
    from plugins.result_storage.model import ResultModel
    datamodel = wwwutils.DistinctSQLAInterface(ResultModel)

    page_size = PAGE_SIZE

    base_permissions = ['can_list', 'can_show']

    list_widget = CurveAnalysisListWidget

    list_title = lazy_gettext("Analysis Via Track No")

    list_columns = ['car_code']

    search_columns = ['car_code']

    label_columns = {
        'car_code': lazy_gettext('Car Code')
    }

    base_filters = [['car_code', TrackNoNotNullFilter, lambda: []]]

    base_order = ('car_code', 'asc')


class CurveAnalysisBoltNoView(CurveAnalysisTrackNoView):
    route_base = '/curves_analysis_bolt'

    list_title = lazy_gettext("Analysis Via Bolt No")

    list_columns = ['bolt_number']

    search_columns = ['bolt_number']

    label_columns = {
        'bolt_number': lazy_gettext('Bolt Number')
    }

    base_filters = [['bolt_number', BoltNoNotNullFilter, lambda: []]]

    base_order = ('bolt_number', 'asc')


curve_ana_controller_view = CurveAnalysisControllerView()
curve_ana_controller_package = {"name": gettext("Analysis Via Controller"),
                                "category": gettext("Analysis"),
                                "view": curve_ana_controller_view}

curve_ana_track_no_view = CurveAnalysisTrackNoView()
curve_ana_track_no_package = {"name": gettext("Analysis Via Track No"),
                              "category": gettext("Analysis"),
                              "view": curve_ana_track_no_view}

curve_ana_bolt_no_view = CurveAnalysisBoltNoView()
curve_ana_bolt_no_package = {"name": gettext("Analysis Via Bolt No"),
                             "category": gettext("Analysis"),
                             "view": curve_ana_bolt_no_view}


class CurveAnalysisControllerViewPlugin(AirflowPlugin):
    name = "curve_analysis_controller_view"
    appbuilder_views = [curve_ana_controller_package, curve_ana_track_no_package, curve_ana_bolt_no_package]
