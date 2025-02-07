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

!function(o){"object"==typeof exports&&"object"==typeof module?o(require("../../lib/codemirror")):"function"==typeof define&&define.amd?define(["../../lib/codemirror"],o):o(CodeMirror)}((function(o){"use strict";o.registerHelper("lint","yaml",(function(e){var r=[];if(!window.jsyaml)return window.console&&window.console.error("Error: window.jsyaml not defined, CodeMirror YAML linting cannot run."),r;try{jsyaml.loadAll(e)}catch(e){var n=e.mark,i=n?o.Pos(n.line,n.column):o.Pos(0,0),t=i;r.push({from:i,to:t,message:e.message})}return r}))}));
