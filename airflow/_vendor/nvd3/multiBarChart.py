#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from .NVD3Chart import NVD3Chart, TemplateMixin


class multiBarChart(TemplateMixin, NVD3Chart):
    """
    A multiple bar graph contains comparisons of two or more categories or bars.
    One axis represents a quantity and the other axis identifies a specific feature
    about the categories. Reading a multiple bar graph includes looking at extremes
    (tallest/longest vs. shortest) in each grouping.

    Python example::

        from nvd3 import multiBarChart
        chart = multiBarChart(width=500, height=400, x_axis_format=None)
        xdata = ['one', 'two', 'three', 'four']
        ydata1 = [6, 12, 9, 16]
        ydata2 = [8, 14, 7, 11]

        chart.add_serie(name="Serie 1", y=ydata1, x=xdata)
        chart.add_serie(name="Serie 2", y=ydata2, x=xdata)
        chart.buildhtml()

    Javascript generated:

    .. raw:: html

        <div id="multiBarChart"><svg style="height:450px; width:100%"></svg></div>
        <script>

            data_multiBarChart=[{"values":
                                [{"y": 6, "x": "one"},
                                {"y": 12, "x": "two"},
                                {"y": 9, "x": "three"},
                                {"y": 16, "x": "four"}],
                                "key": "Serie 1", "yAxis": "1"},
                                {"values":
                                    [{"y": 8, "x": "one"},
                                    {"y": 14, "x": "two"},
                                    {"y": 7, "x": "three"},
                                    {"y": 11, "x": "four"}],
                                "key": "Serie 2", "yAxis": "1"}];

            nv.addGraph(function() {
                var chart = nv.models.multiBarChart();
                chart.margin({top: 30, right: 60, bottom: 20, left: 60});
                var datum = data_multiBarChart;
                        chart.yAxis
                            .tickFormat(d3.format(',.2f'));
                    chart.showLegend(true);
                d3.select('#multiBarChart svg')
                    .datum(datum)
                    .transition().duration(500)
                    .attr('width', 500)
                    .attr('height', 400)
                    .call(chart);
            });


        </script>

    """

    CHART_FILENAME = "./multibarchart.html"
    template_chart_nvd3 = NVD3Chart.template_environment.get_template(CHART_FILENAME)

    def __init__(self, **kwargs):
        super(multiBarChart, self).__init__(**kwargs)

        height = kwargs.get('height', 450)
        width = kwargs.get('width', None)

        if kwargs.get('x_is_date', False):
            self.set_date_flag(True)
            self.create_x_axis('xAxis',
                               format=kwargs.get('x_axis_format', '%d %b %Y'),
                               date=True)
            self.set_custom_tooltip_flag(True)
        else:
            self.create_x_axis('xAxis', format=kwargs.get('x_axis_format', '.2f'))
        self.create_y_axis('yAxis', format=kwargs.get('y_axis_format', '.2f'))

        self.set_graph_height(height)
        if width:
            self.set_graph_width(width)
