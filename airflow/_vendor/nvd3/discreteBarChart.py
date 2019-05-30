#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from .NVD3Chart import NVD3Chart, TemplateMixin


class discreteBarChart(TemplateMixin, NVD3Chart):
    """
    A discrete bar chart or bar graph is a chart with rectangular bars with
    lengths proportional to the values that they represent.

    Python example::

        from nvd3 import discreteBarChart
        chart = discreteBarChart(name='discreteBarChart', height=400, width=400)

        xdata = ["A", "B", "C", "D", "E", "F"]
        ydata = [3, 4, 0, -3, 5, 7]

        chart.add_serie(y=ydata, x=xdata)
        chart.buildhtml()

    Javascript generated:

    .. raw:: html

        <div id="discreteBarChart"><svg style="height:450px; width:100%"></svg></div>
        <script>
            data_discreteBarChart=[{"values": [{"y": 3, "x": "A"}, {"y": 4, "x": "B"}, {"y": 0, "x": "C"}, {"y": -3, "x": "D"}, {"y": 5, "x": "E"}, {"y": 7, "x": "F"}], "key": "Serie 1", "yAxis": "1"}];

            nv.addGraph(function() {
                var chart = nv.models.discreteBarChart();

                chart.margin({top: 30, right: 60, bottom: 20, left: 60});

                var datum = data_discreteBarChart;
                        chart.yAxis
                            .tickFormat(d3.format(',.0f'));
                        chart.tooltipContent(function(key, y, e, graph) {
                            var x = String(graph.point.x);
                            var y = String(graph.point.y);
                            var y = String(graph.point.y);

                            tooltip_str = '<center><b>'+key+'</b></center>' + y + ' at ' + x;
                            return tooltip_str;
                        });

                d3.select('#discreteBarChart svg')
                    .datum(datum)
                    .transition().duration(500)
                    .attr('width', 400)
                    .attr('height', 400)
                    .call(chart);
            });
        </script>


    """
    CHART_FILENAME = "./discretebarchart.html"
    template_chart_nvd3 = NVD3Chart.template_environment.get_template(CHART_FILENAME)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.model = 'discreteBarChart'
        height = kwargs.get('height', 450)
        width = kwargs.get('width', None)

        if kwargs.get('x_is_date', False):
            self.set_date_flag(True)
            self.create_x_axis('xAxis',
                               format=kwargs.get('x_axis_format',
                                                 "%d %b %Y %H %S"),
                               date=True)
        else:
            self.create_x_axis('xAxis', format=None)

        self.create_y_axis('yAxis', format=kwargs.get('y_axis_format', ".0f"))

        self.set_custom_tooltip_flag(True)

        self.set_graph_height(height)
        if width:
            self.set_graph_width(width)
