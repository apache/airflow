#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from .NVD3Chart import NVD3Chart, TemplateMixin


class lineChart(TemplateMixin, NVD3Chart):

    """
    A line chart or line graph is a type of chart which displays information
    as a series of data points connected by straight line segments.

    Python example::

        from nvd3 import lineChart
        chart = lineChart(name="lineChart", x_is_date=False, x_axis_format="AM_PM")

        xdata = range(24)
        ydata = [0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 4, 3, 3, 5, 7, 5, 3, 16, 6, 9, 15, 4, 12]
        ydata2 = [9, 8, 11, 8, 3, 7, 10, 8, 6, 6, 9, 6, 5, 4, 3, 10, 0, 6, 3, 1, 0, 0, 0, 1]

        extra_serie = {"tooltip": {"y_start": "There are ", "y_end": " calls"}}
        chart.add_serie(y=ydata, x=xdata, name='sine', extra=extra_serie, **kwargs1)
        extra_serie = {"tooltip": {"y_start": "", "y_end": " min"}}
        chart.add_serie(y=ydata2, x=xdata, name='cose', extra=extra_serie, **kwargs2)
        chart.buildhtml()

    Javascript renderd to:

    .. raw:: html

        <div id="lineChart"><svg style="height:450px; width:100%"></svg></div>
        <script>

            data_lineChart=[{"values": [{"y": 0, "x": 0}, {"y": 0, "x": 1}, {"y": 1, "x": 2}, {"y": 1, "x": 3}, {"y": 0, "x": 4}, {"y": 0, "x": 5}, {"y": 0, "x": 6}, {"y": 0, "x": 7}, {"y": 1, "x": 8}, {"y": 0, "x": 9}, {"y": 0, "x": 10}, {"y": 4, "x": 11}, {"y": 3, "x": 12}, {"y": 3, "x": 13}, {"y": 5, "x": 14}, {"y": 7, "x": 15}, {"y": 5, "x": 16}, {"y": 3, "x": 17}, {"y": 16, "x": 18}, {"y": 6, "x": 19}, {"y": 9, "x": 20}, {"y": 15, "x": 21}, {"y": 4, "x": 22}, {"y": 12, "x": 23}], "key": "sine", "yAxis": "1"}, {"values": [{"y": 9, "x": 0}, {"y": 8, "x": 1}, {"y": 11, "x": 2}, {"y": 8, "x": 3}, {"y": 3, "x": 4}, {"y": 7, "x": 5}, {"y": 10, "x": 6}, {"y": 8, "x": 7}, {"y": 6, "x": 8}, {"y": 6, "x": 9}, {"y": 9, "x": 10}, {"y": 6, "x": 11}, {"y": 5, "x": 12}, {"y": 4, "x": 13}, {"y": 3, "x": 14}, {"y": 10, "x": 15}, {"y": 0, "x": 16}, {"y": 6, "x": 17}, {"y": 3, "x": 18}, {"y": 1, "x": 19}, {"y": 0, "x": 20}, {"y": 0, "x": 21}, {"y": 0, "x": 22}, {"y": 1, "x": 23}], "key": "cose", "yAxis": "1"}];

            nv.addGraph(function() {
                var chart = nv.models.lineChart();
                chart.margin({top: 30, right: 60, bottom: 20, left: 60});
                var datum = data_lineChart;
                        chart.xAxis
                            .tickFormat(function(d) { return get_am_pm(parseInt(d)); });
                        chart.yAxis
                            .tickFormat(d3.format(',.02f'));

                        chart.tooltipContent(function(key, y, e, graph) {
                            var x = String(graph.point.x);
                            var y = String(graph.point.y);
                                                if(key == 'sine'){
                                var y = 'There is ' +  String(graph.point.y)  + ' calls';
                            }
                            if(key == 'cose'){
                                var y =  String(graph.point.y)  + ' min';
                            }

                            tooltip_str = '<center><b>'+key+'</b></center>' + y + ' at ' + x;
                            return tooltip_str;
                        });
                    chart.showLegend(true);
                        function get_am_pm(d){
                    if (d > 12) {
                        d = d - 12; return (String(d) + 'PM');
                    }
                    else {
                        return (String(d) + 'AM');
                    }
                };

                d3.select('#lineChart svg')
                    .datum(datum)
                    .transition().duration(500)
                    .attr('height', 200)
                    .call(chart);
            return chart;
        });
        </script>

    See the source code of this page, to see the underlying javascript.
    """
    CHART_FILENAME = "./linechart.html"
    template_chart_nvd3 = NVD3Chart.template_environment.get_template(CHART_FILENAME)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.model = 'lineChart'

        height = kwargs.get('height', 450)
        width = kwargs.get('width', None)

        if kwargs.get('x_is_date', False):
            self.set_date_flag(True)
            self.create_x_axis('xAxis',
                               format=kwargs.get('x_axis_format', '%d %b %Y'),
                               date=True)
            self.set_custom_tooltip_flag(True)
        else:
            if kwargs.get('x_axis_format') == 'AM_PM':
                self.x_axis_format = format = 'AM_PM'
            else:
                format = kwargs.get('x_axis_format', 'r')
            self.create_x_axis('xAxis', format=format,
                               custom_format=kwargs.get('x_custom_format',
                                                        False))
        self.create_y_axis(
            'yAxis',
            format=kwargs.get('y_axis_format', '.02f'),
            custom_format=kwargs.get('y_custom_format', False))

        # must have a specified height, otherwise it superimposes both chars
        self.set_graph_height(height)
        if width:
            self.set_graph_width(width)
