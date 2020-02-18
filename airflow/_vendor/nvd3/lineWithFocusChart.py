#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from .NVD3Chart import NVD3Chart, TemplateMixin


class lineWithFocusChart(TemplateMixin, NVD3Chart):
    """
    A lineWithFocusChart or line graph is a type of chart which displays information
    as a series of data points connected by straight line segments.
    The lineWithFocusChart provide a smaller chart that act as a selector,
    this is very useful if you want to zoom on a specific time period.

    Python example::

        from nvd3 import lineWithFocusChart
        chart = lineWithFocusChart(name='lineWithFocusChart', x_is_date=True, x_axis_format="%d %b %Y")
        xdata = [1365026400000000, 1365026500000000, 1365026600000000, 1365026700000000, 1365026800000000, 1365026900000000, 1365027000000000]
        ydata = [-6, 5, -1, 2, 4, 8, 10]

        extra_serie = {"tooltip": {"y_start": "", "y_end": " ext"},
                       "date_format": "%d %b %Y"}
        chart.add_serie(name="Serie 1", y=ydata, x=xdata, extra=extra_serie)
        chart.buildhtml()

    Javascript generated:

    .. raw:: html

        <div id="lineWithFocusChart"><svg style="height:450px; width:100%"></svg></div>
        <script>
            data_lineWithFocusChart=[{"values": [{"y": -6, "x": 1365026400000000}, {"y": 5, "x": 1365026500000000}, {"y": -1, "x": 1365026600000000}], "key": "Serie 1", "yAxis": "1"}];
            nv.addGraph(function() {
                var chart = nv.models.lineWithFocusChart();
                chart.margin({top: 30, right: 60, bottom: 20, left: 60});
                var datum = data_lineWithFocusChart;
                        chart.yAxis
                            .tickFormat(d3.format(',.2f'));
                        chart.y2Axis
                            .tickFormat(d3.format(',.2f'));
                        chart.xAxis
                            .tickFormat(function(d) { return d3.time.format('%d %b %Y')(new Date(parseInt(d))) });
                        chart.x2Axis
                            .tickFormat(function(d) { return d3.time.format('%d %b %Y')(new Date(parseInt(d))) });

                    chart.tooltipContent(function(key, y, e, graph) {
                        var x = d3.time.format("%d %b %Y")(new Date(parseInt(graph.point.x)));
                        var y = String(graph.point.y);
                                            if(key == 'Serie 1'){
                                var y =  String(graph.point.y)  + ' ext';
                            }

                        tooltip_str = '<center><b>'+key+'</b></center>' + y + ' on ' + x;
                        return tooltip_str; });

                    chart.showLegend(true);

                d3.select('#lineWithFocusChart svg')
                    .datum(datum)
                    .transition().duration(500)
                    .attr('height', 450)
                    .call(chart); });
        </script>

    """

    CHART_FILENAME = "./linewfocuschart.html"
    template_chart_nvd3 = NVD3Chart.template_environment.get_template(CHART_FILENAME)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.model = 'lineWithFocusChart'

        height = kwargs.get('height', 450)
        width = kwargs.get('width', None)

        if kwargs.get('x_is_date', False):
            self.set_date_flag(True)
            self.create_x_axis('xAxis', format=kwargs.get('x_axis_format',
                                                          '%d %b %Y %H %S'),
                               date=True)
            self.create_x_axis('x2Axis', format=kwargs.get('x_axis_format',
                                                           '%d %b %Y %H %S'),
                               date=True)
            self.set_custom_tooltip_flag(True)
        else:
            self.create_x_axis('xAxis', format=kwargs.get('x_axis_format',
                                                          '.2f'))
            self.create_x_axis('x2Axis', format=kwargs.get('x_axis_format',
                                                           '.2f'))

        self.create_y_axis('yAxis', format=kwargs.get('y_axis_format', '.2f'))
        self.create_y_axis('y2Axis', format=kwargs.get('y_axis_format', '.2f'))

        self.set_graph_height(height)
        if width:
            self.set_graph_width(width)
