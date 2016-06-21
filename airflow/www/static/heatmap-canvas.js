/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/


    /**
     * This plugin extends Highcharts in two ways:
     * - Use HTML5 canvas instead of SVG for rendering of the heatmap squares. Canvas
     *   outperforms SVG when it comes to thousands of single shapes.
     * - Add a K-D-tree to find the nearest point on mouse move. Since we no longer have SVG shapes
     *   to capture mouseovers, we need another way of detecting hover points for the tooltip.
     */
    (function (H) {
        var wrap = H.wrap,
            seriesTypes = H.seriesTypes;

        /**
         * Recursively builds a K-D-tree
         */
        function KDTree(points, depth) {
            var axis, median, length = points && points.length;

            if (length) {

                // alternate between the axis
                axis = ['plotX', 'plotY'][depth % 2];

                // sort point array
                points.sort(function (a, b) {
                    return a[axis] - b[axis];
                });

                median = Math.floor(length / 2);

                // build and return node
                return {
                    point: points[median],
                    left: KDTree(points.slice(0, median), depth + 1),
                    right: KDTree(points.slice(median + 1), depth + 1)
                };

            }
        }

        /**
         * Recursively searches for the nearest neighbour using the given K-D-tree
         */
        function nearest(search, tree, depth) {
            var point = tree.point,
                axis = ['plotX', 'plotY'][depth % 2],
                tdist,
                sideA,
                sideB,
                ret = point,
                nPoint1,
                nPoint2;

            // Get distance
            point.dist = Math.pow(search.plotX - point.plotX, 2) +
                Math.pow(search.plotY - point.plotY, 2);

            // Pick side based on distance to splitting point
            tdist = search[axis] - point[axis];
            sideA = tdist < 0 ? 'left' : 'right';

            // End of tree
            if (tree[sideA]) {
                nPoint1 = nearest(search, tree[sideA], depth + 1);

                ret = (nPoint1.dist < ret.dist ? nPoint1 : point);

                sideB = tdist < 0 ? 'right' : 'left';
                if (tree[sideB]) {
                    // compare distance to current best to splitting point to decide wether to check side B or not
                    if (Math.abs(tdist) < ret.dist) {
                        nPoint2 = nearest(search, tree[sideB], depth + 1);
                        ret = (nPoint2.dist < ret.dist ? nPoint2 : ret);
                    }
                }
            }
            return ret;
        }

        // Extend the heatmap to use the K-D-tree to search for nearest points
        H.seriesTypes.heatmap.prototype.setTooltipPoints = function () {
            var series = this;

            this.tree = null;
            setTimeout(function () {
                series.tree = KDTree(series.points, 0);
            });
        };
        H.seriesTypes.heatmap.prototype.getNearest = function (search) {
            if (this.tree) {
                return nearest(search, this.tree, 0);
            }
        };

        H.wrap(H.Pointer.prototype, 'runPointActions', function (proceed, e) {
            var chart = this.chart;
            proceed.call(this, e);

            // Draw independent tooltips
            H.each(chart.series, function (series) {
                var point;
                if (series.getNearest) {
                    point = series.getNearest({
                        plotX: e.chartX - chart.plotLeft,
                        plotY: e.chartY - chart.plotTop
                    });
                    if (point) {
                        point.onMouseOver(e);
                    }
                }
            })
        });

        /**
         * Get the canvas context for a series
         */
        H.Series.prototype.getContext = function () {
            var canvas;
            if (!this.ctx) {
                canvas = document.createElement('canvas');
                canvas.setAttribute('width', this.chart.plotWidth);
                canvas.setAttribute('height', this.chart.plotHeight);
                canvas.style.position = 'absolute';
                canvas.style.left = this.group.translateX + 'px';
                canvas.style.top = this.group.translateY + 'px';
                canvas.style.zIndex = 0;
                canvas.style.cursor = 'crosshair';
                this.chart.container.appendChild(canvas);
                if (canvas.getContext) {
                    this.ctx = canvas.getContext('2d');
                }
            }
            return this.ctx;
        }

        /**
         * Wrap the drawPoints method to draw the points in canvas instead of the slower SVG,
         * that requires one shape each point.
         */
        H.wrap(H.seriesTypes.heatmap.prototype, 'drawPoints', function (proceed) {

            var ctx;
            if (this.chart.renderer.forExport) {
                // Run SVG shapes
                proceed.call(this);

            } else {

                if (ctx = this.getContext()) {

                    // draw the columns
                    H.each(this.points, function (point) {
                        var plotY = point.plotY,
                            shapeArgs;

                        if (plotY !== undefined && !isNaN(plotY) && point.y !== null) {
                            shapeArgs = point.shapeArgs;

                            ctx.fillStyle = point.pointAttr[''].fill;
                            ctx.fillRect(shapeArgs.x, shapeArgs.y, shapeArgs.width, shapeArgs.height);
                        }
                    });

                } else {
                    this.chart.showLoading("Your browser doesn't support HTML5 canvas, <br>please use a modern browser");

                    // Uncomment this to provide low-level (slow) support in oldIE. It will cause script errors on
                    // charts with more than a few thousand points.
                    //proceed.call(this);
                }
            }
        });
    }(Highcharts));


