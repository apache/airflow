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

!function(e){"object"==typeof exports&&"object"==typeof module?e(require("../../lib/codemirror")):"function"==typeof define&&define.amd?define(["../../lib/codemirror"],e):e(CodeMirror)}((function(e){"use strict";e.registerHelper("lint","javascript",(function(r,n){if(!window.JSHINT)return window.console&&window.console.error("Error: window.JSHINT not defined, CodeMirror JavaScript linting cannot run."),[];n.indent||(n.indent=1),JSHINT(r,n,n.globals);var o=JSHINT.data().errors,i=[];return o&&function(r,n){for(var o=0;o<r.length;o++){var i=r[o];if(i){if(i.line<=0){window.console&&window.console.warn("Cannot display JSHint error (invalid line "+i.line+")",i);continue}var t=i.character-1,a=t+1;if(i.evidence){var d=i.evidence.substring(t).search(/.\b/);d>-1&&(a+=d)}var c={message:i.reason,severity:i.code&&i.code.startsWith("W")?"warning":"error",from:e.Pos(i.line-1,t),to:e.Pos(i.line-1,a)};n.push(c)}}}(o,i),i}))}));
