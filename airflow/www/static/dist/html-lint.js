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

!function(e){"object"==typeof exports&&"object"==typeof module?e(require("../../lib/codemirror"),require("htmlhint")):"function"==typeof define&&define.amd?define(["../../lib/codemirror","htmlhint"],e):e(CodeMirror,window.HTMLHint)}((function(e,r){"use strict";var o={"tagname-lowercase":!0,"attr-lowercase":!0,"attr-value-double-quotes":!0,"doctype-first":!1,"tag-pair":!0,"spec-char-escape":!0,"id-unique":!0,"src-not-empty":!0,"attr-no-duplication":!0};e.registerHelper("lint","html",(function(t,i){var n=[];if(r&&!r.verify&&(r=void 0!==r.default?r.default:r.HTMLHint),r||(r=window.HTMLHint),!r)return window.console&&window.console.error("Error: HTMLHint not found, not defined on window, or not available through define/require, CodeMirror HTML linting cannot run."),n;for(var a=r.verify(t,i&&i.rules||o),l=0;l<a.length;l++){var d=a[l],u=d.line-1,c=d.line-1,s=d.col-1,f=d.col;n.push({from:e.Pos(u,s),to:e.Pos(c,f),message:d.message,severity:d.type})}return n}))}));
