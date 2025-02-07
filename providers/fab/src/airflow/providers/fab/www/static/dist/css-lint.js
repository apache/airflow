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

!function(e){"object"==typeof exports&&"object"==typeof module?e(require("../../lib/codemirror")):"function"==typeof define&&define.amd?define(["../../lib/codemirror"],e):e(CodeMirror)}((function(e){"use strict";e.registerHelper("lint","css",(function(o,r){var n=[];if(!window.CSSLint)return window.console&&window.console.error("Error: window.CSSLint not defined, CodeMirror CSS linting cannot run."),n;for(var i=CSSLint.verify(o,r).messages,t=null,s=0;s<i.length;s++){var c=(t=i[s]).line-1,d=t.line-1,f=t.col-1,l=t.col;n.push({from:e.Pos(c,f),to:e.Pos(d,l),message:t.message,severity:t.type})}return n}))}));
