/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const webpack = require("webpack");
const path = require("path");
const { WebpackManifestPlugin } = require("webpack-manifest-plugin");
const cwplg = require("clean-webpack-plugin");
const CopyWebpackPlugin = require("copy-webpack-plugin");
const MiniCssExtractPlugin = require("mini-css-extract-plugin");
const MomentLocalesPlugin = require("moment-locales-webpack-plugin");
const CssMinimizerPlugin = require("css-minimizer-webpack-plugin");
const LicensePlugin = require("webpack-license-plugin");
const TerserPlugin = require("terser-webpack-plugin");

// Input Directory (airflow/www)
// noinspection JSUnresolvedVariable
const CSS_DIR = path.resolve(__dirname, "./static/css");
const JS_DIR = path.resolve(__dirname, "./static/js");

// Output Directory (airflow/www/static/dist)
// noinspection JSUnresolvedVariable
const BUILD_DIR = path.resolve(__dirname, "./static/dist");

// Convert licenses json into a standard format for LICENSES.txt
const formatLicenses = (packages) => {
  let text = `Apache Airflow
Copyright 2016-2021 The Apache Software Foundation

This product includes software developed at The Apache Software
Foundation (http://www.apache.org/).

=======================================================================
`;
  packages.forEach((p) => {
    text += `${p.name}|${p.version}:\n-----\n${p.license}\n${
      p.licenseText || p.author
    }\n${p.repository || ""}\n\n\n`;
  });
  return text;
};

const config = {
  entry: {
    airflowDefaultTheme: `${CSS_DIR}/bootstrap-theme.css`,
    connectionForm: `${JS_DIR}/connection_form.js`,
    chart: [`${CSS_DIR}/chart.css`],
    dag: `${JS_DIR}/dag.js`,
    dagDependencies: `${JS_DIR}/dag_dependencies.js`,
    dags: [`${CSS_DIR}/dags.css`, `${JS_DIR}/dags.js`],
    flash: `${CSS_DIR}/flash.css`,
    graph: `${CSS_DIR}/graph.css`,
    loadingDots: `${CSS_DIR}/loading-dots.css`,
    main: [`${CSS_DIR}/main.css`, `${JS_DIR}/main.js`],
    materialIcons: `${CSS_DIR}/material-icons.css`,
    moment: "moment-timezone",
    switch: `${CSS_DIR}/switch.css`,
    task: `${JS_DIR}/task.js`,
    taskInstances: `${JS_DIR}/task_instances.js`,
    tiLog: `${JS_DIR}/ti_log.js`,
    grid: `${JS_DIR}/dag/index.tsx`,
    clusterActivity: `${JS_DIR}/cluster-activity/index.tsx`,
    datasets: `${JS_DIR}/datasets/index.tsx`,
    calendar: [`${CSS_DIR}/calendar.css`, `${JS_DIR}/calendar.js`],
    durationChart: `${JS_DIR}/duration_chart.js`,
    trigger: `${JS_DIR}/trigger.js`,
    variableEdit: `${JS_DIR}/variable_edit.js`,
  },
  output: {
    path: BUILD_DIR,
    filename: "[name].[chunkhash].js",
    chunkFilename: "[name].[chunkhash].js",
    library: ["Airflow", "[name]"],
    libraryTarget: "umd",
    publicPath: "",
  },
  resolve: {
    alias: {
      // Be sure to update aliases in jest.config.js and tsconfig.json
      src: path.resolve(__dirname, "static/js"),
    },
    extensions: [".js", ".jsx", ".ts", ".tsx", ".css"],
  },
  module: {
    rules: [
      {
        test: /datatables\.net.*/,
        use: [
          {
            loader: "imports-loader?define=>false",
          },
        ],
      },
      {
        test: /\.(js|jsx|tsx|ts)$/,
        exclude: /node_modules/,
        use: [
          {
            loader: "babel-loader",
            options: {
              presets: ["@babel/preset-react", "@babel/preset-typescript"],
            },
          },
        ],
      },
      // Extract css files
      {
        test: /\.css$/,
        include: CSS_DIR,
        exclude: /node_modules/,
        use: [
          {
            loader: MiniCssExtractPlugin.loader,
            options: {
              esModule: true,
            },
          },
          "css-loader",
        ],
      },
      // Extract css files
      {
        test: /\.css$/,
        exclude: CSS_DIR,
        include: /node_modules/,
        use: ["css-loader"],
      },
      /* for css linking images */
      {
        test: /\.(png|jpg|gif)$/i,
        use: [
          {
            loader: "url-loader",
            options: {
              limit: 100000,
            },
          },
        ],
      },
      /* for fonts */
      {
        test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        use: [
          {
            loader: "url-loader",
            options: {
              limit: 100000,
              mimetype: "application/font-woff",
            },
          },
        ],
      },
      {
        test: /\.(ttf|eot|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        use: [
          {
            loader: "file-loader",
          },
        ],
      },
    ],
  },
  plugins: [
    new WebpackManifestPlugin({
      // d3-tip is named index.js in its dist folder which was confusing the manifest
      map: (file) =>
        file.path === "d3-tip.js" ? { ...file, name: "d3-tip.js" } : file,
    }),
    new cwplg.CleanWebpackPlugin({
      verbose: true,
    }),
    new MiniCssExtractPlugin({
      filename: "[name].[chunkhash].css",
    }),

    // MomentJS loads all the locale, making it a huge JS file.
    // This will ignore the locales from momentJS
    new MomentLocalesPlugin(),

    new webpack.DefinePlugin({
      "process.env": {
        NODE_ENV: JSON.stringify(process.env.NODE_ENV),
      },
    }),
    // Since we have all the dependencies separated from hard-coded JS within HTML,
    // this seems like an efficient solution for now. Will update that once
    // we'll have the dependencies imported within the custom JS
    new CopyWebpackPlugin({
      patterns: [
        {
          from: "node_modules/nvd3/build/*.min.*",
          flatten: true,
        },
        {
          from: "node_modules/d3/d3.min.*",
          flatten: true,
        },
        {
          from: "node_modules/dagre-d3/dist/*.min.*",
          flatten: true,
        },
        {
          from: "node_modules/d3-shape/dist/*.min.*",
          flatten: true,
        },
        {
          from: "node_modules/d3-tip/dist/index.js",
          to: "d3-tip.js",
          flatten: true,
        },
        {
          from: "node_modules/bootstrap-3-typeahead/*min.*",
          flatten: true,
        },
        {
          from: "node_modules/datatables.net/**/**.min.*",
          flatten: true,
        },
        {
          from: "node_modules/datatables.net-bs/**/**.min.*",
          flatten: true,
        },
        {
          from: "node_modules/eonasdan-bootstrap-datetimepicker/build/css/bootstrap-datetimepicker.min.css",
          flatten: true,
        },
        {
          from: "node_modules/eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min.js",
          flatten: true,
        },
        {
          from: "node_modules/redoc/bundles/redoc.standalone.*",
          flatten: true,
        },
        {
          from: "node_modules/codemirror/lib/codemirror.*",
          flatten: true,
        },
        {
          from: "node_modules/codemirror/addon/lint/**.*",
          flatten: true,
        },
        {
          from: "node_modules/codemirror/mode/javascript/javascript.js",
          flatten: true,
        },
        {
          from: "node_modules/jshint/dist/jshint.js",
          flatten: true,
        },
        {
          from: "templates/swagger-ui",
          to: `${BUILD_DIR}/swagger-ui`,
        },
        {
          from: "node_modules/swagger-ui-dist",
          to: `${BUILD_DIR}/swagger-ui`,
        },
      ],
    }),
    new LicensePlugin({
      additionalFiles: {
        "../../../../licenses/LICENSES-ui.txt": formatLicenses,
      },
      unacceptableLicenseTest: (licenseIdentifier) =>
        [
          "BCL",
          "JSR",
          "ASL",
          "RSAL",
          "SSPL",
          "CPOL",
          "NPL",
          "BSD-4",
          "QPL",
          "GPL",
          "LGPL",
        ].includes(licenseIdentifier),
    }),
  ],
  optimization: {
    minimize: process.env.NODE_ENV === "production",
    minimizer: [new CssMinimizerPlugin({}), new TerserPlugin()],
  },
};

module.exports = config;
