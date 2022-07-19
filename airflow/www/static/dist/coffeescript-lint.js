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

!function(e){"object"==typeof exports&&"object"==typeof module?e(require("../../lib/codemirror")):"function"==typeof define&&define.amd?define(["../../lib/codemirror"],e):e(CodeMirror)}((function(e){"use strict";e.registerHelper("lint","coffeescript",(function(o){var r=[];if(!window.coffeelint)return window.console&&window.console.error("Error: window.coffeelint not defined, CodeMirror CoffeeScript linting cannot run."),r;var i,n;try{for(var t=coffeelint.lint(o),f=0;f<t.length;f++)i=t[f],n=void 0,n=i.lineNumber,r.push({from:e.Pos(n-1,0),to:e.Pos(n,0),severity:i.level,message:i.message})}catch(o){r.push({from:e.Pos(o.location.first_line,0),to:e.Pos(o.location.last_line,o.location.last_column),severity:"error",message:o.message})}return r}))}));
