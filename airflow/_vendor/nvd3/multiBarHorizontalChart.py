#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from .NVD3Chart import NVD3Chart, TemplateMixin


class multiBarHorizontalChart(TemplateMixin, NVD3Chart):
    """
    A multiple horizontal bar graph contains comparisons of two or more categories or bars.

    Python example::

        from nvd3 import multiBarHorizontalChart
        chart = multiBarHorizontalChart(name='multiBarHorizontalChart', height=400, width=400)
        xdata = [-14, -7, 7, 14]
        ydata = [-6, 5, -1, 9]
        y2data = [-23, -6, -32, 9]

        extra_serie = {"tooltip": {"y_start": "", "y_end": " balls"}}
        chart.add_serie(name="Serie 1", y=ydata, x=xdata, extra=extra_serie)

        extra_serie = {"tooltip": {"y_start": "", "y_end": " calls"}}
        chart.add_serie(name="Serie 2", y=y2data, x=xdata, extra=extra_serie)
        chart.buildcontent()

    Javascript generated:

    .. raw:: html

        <div id="multiBarHorizontalChart"><svg style="height:450px; width:100%"></svg></div>
        <script>

            data_multiBarHorizontalChart=[{"values":
                [{"y": -6, "x": -14}, {"y": 5, "x": -7}, {"y": -1, "x": 7}, {"y": 9, "x": 14}],
                "key": "Serie 1", "yAxis": "1"},
                {"values":
                    [{"y": -23, "x": -14}, {"y": -6, "x": -7}, {"y": -32, "x": 7}, {"y": 9, "x": 14}],
                "key": "Serie 2", "yAxis": "1"}];

            nv.addGraph(function() {
                var chart = nv.models.multiBarHorizontalChart();

                chart.margin({top: 30, right: 60, bottom: 20, left: 60});

                var datum = data_multiBarHorizontalChart;

                        chart.xAxis
                            .tickFormat(d3.format(',.2f'));
                        chart.yAxis
                            .tickFormat(d3.format(',.2f'));

                        chart.tooltipContent(function(key, y, e, graph) {
                            var x = String(graph.point.x);
                            var y = String(graph.point.y);
                                                if(key == 'Serie 1'){
                                var y =  String(graph.point.y)  + ' balls';
                            }
                            if(key == 'Serie 2'){
                                var y =  String(graph.point.y)  + ' calls';
                            }

                            tooltip_str = '<center><b>'+key+'</b></center>' + y + ' at ' + x;
                            return tooltip_str;
                        });

                    chart.showLegend(true);

                d3.select('#multiBarHorizontalChart svg')
                    .datum(datum)
                    .transition().duration(500)
                    .attr('width', 400)
                    .attr('height', 400)
                    .call(chart);
            });
        </script>

    """

    CHART_FILENAME = "./multibarcharthorizontal.html"
    template_chart_nvd3 = NVD3Chart.template_environment.get_template(CHART_FILENAME)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        height = kwargs.get('height', 450)
        width = kwargs.get('width', None)

        self.create_x_axis('xAxis', format=kwargs.get('x_axis_format', '.2f'))
        self.create_y_axis('yAxis', format=kwargs.get('y_axis_format', '.2f'))

        self.set_graph_height(height)
        if width:
            self.set_graph_width(width)
