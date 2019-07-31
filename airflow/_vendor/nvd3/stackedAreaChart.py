#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from .NVD3Chart import NVD3Chart, TemplateMixin


class stackedAreaChart(TemplateMixin, NVD3Chart):
    """
    The stacked area chart is identical to the area chart, except the areas are stacked
    on top of each other, rather than overlapping. This can make the chart much easier to read.

    Python example::

        from nvd3 import stackedAreaChart
        chart = stackedAreaChart(name='stackedAreaChart', height=400, width=400)

        xdata = [100, 101, 102, 103, 104, 105, 106,]
        ydata = [6, 11, 12, 7, 11, 10, 11]
        ydata2 = [8, 20, 16, 12, 20, 28, 28]

        extra_serie = {"tooltip": {"y_start": "There is ", "y_end": " min"}}
        chart.add_serie(name="Serie 1", y=ydata, x=xdata, extra=extra_serie)
        chart.add_serie(name="Serie 2", y=ydata2, x=xdata, extra=extra_serie)
        chart.buildhtml()

    Javascript generated:

    .. raw:: html

        <div id="stackedAreaChart"><svg style="height:450px; width:100%"></svg></div>
        <script>


            data_stackedAreaChart=[{"values": [{"y": 6, "x": 100}, {"y": 11, "x": 101}, {"y": 12, "x": 102}, {"y": 7, "x": 103}, {"y": 11, "x": 104}, {"y": 10, "x": 105}, {"y": 11, "x": 106}], "key": "Serie 1", "yAxis": "1"}, {"values": [{"y": 8, "x": 100}, {"y": 20, "x": 101}, {"y": 16, "x": 102}, {"y": 12, "x": 103}, {"y": 20, "x": 104}, {"y": 28, "x": 105}, {"y": 28, "x": 106}], "key": "Serie 2", "yAxis": "1"}];
            nv.addGraph(function() {
                var chart = nv.models.stackedAreaChart();
                chart.margin({top: 30, right: 60, bottom: 20, left: 60});
                var datum = data_stackedAreaChart;
                        chart.xAxis
                            .tickFormat(d3.format(',.2f'));
                        chart.yAxis
                            .tickFormat(d3.format(',.2f'));

                        chart.tooltipContent(function(key, y, e, graph) {
                            var x = String(graph.point.x);
                            var y = String(graph.point.y);
                                                if(key == 'Serie 1'){
                                var y = 'There is ' +  String(graph.point.y)  + ' min';
                            }
                            if(key == 'Serie 2'){
                                var y = 'There is ' +  String(graph.point.y)  + ' min';
                            }

                            tooltip_str = '<center><b>'+key+'</b></center>' + y + ' at ' + x;
                            return tooltip_str;
                        });
                    chart.showLegend(true);
                d3.select('#stackedAreaChart svg')
                    .datum(datum)
                    .transition().duration(500)
                    .attr('width', 400)
                    .attr('height', 400)
                    .call(chart);
            });
        </script>

    """

    CHART_FILENAME = "./stackedareachart.html"
    template_chart_nvd3 = NVD3Chart.template_environment.get_template(CHART_FILENAME)

    def __init__(self, **kwargs):
        super(stackedAreaChart, self).__init__(**kwargs)
        height = kwargs.get('height', 450)
        width = kwargs.get('width', None)
        self.model = 'stackedAreaChart'

        if kwargs.get('x_is_date', False):
            self.set_date_flag(True)
            self.create_x_axis('xAxis',
                               format=kwargs.get('x_axis_format', '%d %b %Y'),
                               date=True)
            self.set_custom_tooltip_flag(True)
        else:
            self.create_x_axis('xAxis', format=kwargs.get('x_axis_format',
                                                          '.2f'))
        self.create_y_axis('yAxis', format=kwargs.get('y_axis_format', '.2f'))

        self.set_graph_height(height)
        if width:
            self.set_graph_width(width)
